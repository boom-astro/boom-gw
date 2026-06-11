//! SkyPortal-style access control: roles → ACLs, group membership, and
//! stream access, resolved per request into an [`AccessContext`].
//!
//! Identity itself is token-derived (see [`crate::auth`]); this module
//! is the *authorization* layer on top of the persisted users, roles,
//! groups, and streams in [`crate::archive`]. The default roles and the
//! five messenger streams are seeded by
//! [`crate::archive::Archive::seed_access_defaults`].

use std::collections::HashSet;

use futures::TryStreamExt;
use mongodb::bson::doc;

use crate::archive::{Archive, ArchiveError, RoleDoc, StreamDoc};

// --- ACL strings -----------------------------------------------------------
// Code-defined because they appear directly in handler gates. Roles
// (which bundle these) live in the `roles` collection and are editable.

/// Wildcard — a principal with this ACL passes every [`AccessContext::has_acl`]
/// check and is treated as admin of every group / stream.
pub const ACL_SYSTEM_ADMIN: &str = "System admin";
pub const ACL_MANAGE_USERS: &str = "Manage users";
pub const ACL_MANAGE_GROUPS: &str = "Manage groups";
pub const ACL_MANAGE_ROLES: &str = "Manage roles";
pub const ACL_MANAGE_STREAMS: &str = "Manage streams";
pub const ACL_MANAGE_SCIENCE_FILTERS: &str = "Manage science filters";
pub const ACL_PUBLISH_ALERTS: &str = "Publish alerts";
pub const ACL_UPLOAD_DATA: &str = "Upload data";

/// Every ACL boom-gw knows about, for `GET /api/acls`.
pub const ALL_ACLS: &[&str] = &[
    ACL_SYSTEM_ADMIN,
    ACL_MANAGE_USERS,
    ACL_MANAGE_GROUPS,
    ACL_MANAGE_ROLES,
    ACL_MANAGE_STREAMS,
    ACL_MANAGE_SCIENCE_FILTERS,
    ACL_PUBLISH_ALERTS,
    ACL_UPLOAD_DATA,
];

// --- Role slugs ------------------------------------------------------------

pub const ROLE_SUPER_ADMIN: &str = "super_admin";
pub const ROLE_GROUP_ADMIN: &str = "group_admin";
pub const ROLE_FULL_USER: &str = "full_user";
pub const ROLE_VIEW_ONLY: &str = "view_only";

// --- Stream slugs (the five messenger ingest channels) ---------------------

pub const STREAM_GRACEDB_GW: &str = "gracedb_gw";
pub const STREAM_GCN_GRB: &str = "gcn_grb";
pub const STREAM_GCN_FRB: &str = "gcn_frb";
pub const STREAM_GCN_NEUTRINO: &str = "gcn_neutrino";
pub const STREAM_BOOM_OPTICAL: &str = "boom_optical";

/// The default, find-or-inserted roles seeded at startup.
pub fn default_roles() -> Vec<RoleDoc> {
    let role = |id: &str, name: &str, acls: &[&str]| RoleDoc {
        id: id.to_string(),
        name: name.to_string(),
        acls: acls.iter().map(|s| s.to_string()).collect(),
        description: String::new(),
        system: true,
    };
    vec![
        role(ROLE_SUPER_ADMIN, "Super admin", &[ACL_SYSTEM_ADMIN]),
        role(
            ROLE_GROUP_ADMIN,
            "Group admin",
            &[
                ACL_MANAGE_GROUPS,
                ACL_MANAGE_SCIENCE_FILTERS,
                ACL_PUBLISH_ALERTS,
                ACL_UPLOAD_DATA,
            ],
        ),
        role(
            ROLE_FULL_USER,
            "Full user",
            &[ACL_MANAGE_SCIENCE_FILTERS, ACL_UPLOAD_DATA],
        ),
        role(ROLE_VIEW_ONLY, "View only", &[]),
    ]
}

/// The default, find-or-inserted messenger streams seeded at startup.
pub fn default_streams() -> Vec<StreamDoc> {
    let stream = |id: &str, name: &str| StreamDoc {
        id: id.to_string(),
        name: name.to_string(),
        description: String::new(),
        system: true,
    };
    vec![
        stream(STREAM_GRACEDB_GW, "GraceDB GW"),
        stream(STREAM_GCN_GRB, "GCN GRB"),
        stream(STREAM_GCN_FRB, "GCN FRB"),
        stream(STREAM_GCN_NEUTRINO, "GCN neutrino"),
        stream(STREAM_BOOM_OPTICAL, "BOOM optical"),
    ]
}

/// Map an external-event instrument label to the messenger stream it
/// belongs to, for stream-gating cross-matches. Mirrors the
/// `messengerStyle` categories used in the web UI. Returns `None` for
/// unrecognized instruments (treated as ungated).
pub fn instrument_stream(instrument: &str) -> Option<&'static str> {
    if instrument.starts_with("Fermi-")
        || instrument.starts_with("Swift-")
        || instrument.starts_with("SVOM-")
        || instrument.starts_with("Einstein")
        || instrument.starts_with("BurstCube")
    {
        Some(STREAM_GCN_GRB)
    } else if instrument == "CHIME-FRB" || instrument == "DSA110-FRB" {
        Some(STREAM_GCN_FRB)
    } else if instrument == "IceCube" || instrument == "KM3NeT" {
        Some(STREAM_GCN_NEUTRINO)
    } else if instrument == "BOOM" {
        Some(STREAM_BOOM_OPTICAL)
    } else {
        None
    }
}

/// JIT-provision a user and apply the site-admin bootstrap. Called at
/// every login (and lazily on the bearer path). `email`/`display_name`
/// are `None` when the caller doesn't have them (dev-login, bearer);
/// they're only ever set, never cleared.
///
/// Bootstrap rule: a `sub` in `site_admins` always holds `super_admin`;
/// if `site_admins` is empty, the very first provisioned user becomes
/// super admin so a fresh deployment isn't locked out.
pub async fn provision_user(
    archive: &Archive,
    site_admins: &HashSet<String>,
    sub: &str,
    email: Option<&str>,
    display_name: Option<&str>,
) -> Result<crate::archive::UserDoc, ArchiveError> {
    let is_site_admin = site_admins.contains(sub);
    let bootstrap = is_site_admin || (site_admins.is_empty() && archive.user_count().await? == 0);
    let mut user = archive
        .upsert_user(sub, email, display_name, bootstrap)
        .await?;
    // Upgrade a configured site admin who was provisioned before being
    // added to the allowlist.
    if is_site_admin && !user.role_ids.iter().any(|r| r == ROLE_SUPER_ADMIN) {
        archive.add_user_role(sub, ROLE_SUPER_ADMIN).await?;
        user.role_ids.push(ROLE_SUPER_ADMIN.to_string());
    }
    Ok(user)
}

/// A principal's effective authorization, resolved from the persisted
/// roles/groups/streams. Loaded per handler via [`Self::load`].
#[derive(Debug, Clone, Default)]
pub struct AccessContext {
    pub sub: String,
    /// Union of ACLs across the user's roles.
    pub acls: HashSet<String>,
    /// Group ids the user belongs to.
    pub group_member: HashSet<String>,
    /// Group ids the user administers.
    pub group_admin: HashSet<String>,
    /// Stream ids the user can access (direct grants ∪ group streams).
    pub stream_ids: HashSet<String>,
    /// True when the user holds [`ACL_SYSTEM_ADMIN`] — short-circuits
    /// every check below.
    pub wildcard: bool,
}

impl AccessContext {
    /// Resolve `sub`'s authorization from the archive (~4 indexed
    /// queries: user → roles → group memberships → direct + group
    /// streams). An unknown/role-less user yields an empty context.
    pub async fn load(archive: &Archive, sub: &str) -> Result<Self, ArchiveError> {
        let role_ids = archive
            .users()
            .find_one(doc! {"_id": sub})
            .await?
            .map(|u| u.role_ids)
            .unwrap_or_default();

        let mut acls = HashSet::new();
        if !role_ids.is_empty() {
            let mut cursor = archive
                .roles()
                .find(doc! {"_id": {"$in": &role_ids}})
                .await?;
            while let Some(role) = cursor.try_next().await? {
                acls.extend(role.acls);
            }
        }
        let wildcard = acls.contains(ACL_SYSTEM_ADMIN);

        let mut group_member = HashSet::new();
        let mut group_admin = HashSet::new();
        let mut cursor = archive.group_users().find(doc! {"user_sub": sub}).await?;
        while let Some(gu) = cursor.try_next().await? {
            if gu.admin {
                group_admin.insert(gu.group_id.clone());
            }
            group_member.insert(gu.group_id);
        }

        let mut stream_ids = HashSet::new();
        let mut cursor = archive.stream_users().find(doc! {"user_sub": sub}).await?;
        while let Some(su) = cursor.try_next().await? {
            stream_ids.insert(su.stream_id);
        }
        if !group_member.is_empty() {
            let gids: Vec<&String> = group_member.iter().collect();
            let mut cursor = archive
                .group_streams()
                .find(doc! {"group_id": {"$in": gids}})
                .await?;
            while let Some(gs) = cursor.try_next().await? {
                stream_ids.insert(gs.stream_id);
            }
        }

        Ok(Self {
            sub: sub.to_string(),
            acls,
            group_member,
            group_admin,
            stream_ids,
            wildcard,
        })
    }

    pub fn has_acl(&self, acl: &str) -> bool {
        self.wildcard || self.acls.contains(acl)
    }

    pub fn in_group(&self, group_id: &str) -> bool {
        self.group_member.contains(group_id)
    }

    pub fn is_group_admin(&self, group_id: &str) -> bool {
        self.wildcard || self.group_admin.contains(group_id)
    }

    pub fn can_access_stream(&self, stream_id: &str) -> bool {
        self.wildcard || self.stream_ids.contains(stream_id)
    }

    /// Group ids for the membership-scoped `$in` query used to list a
    /// user's visible science filters.
    pub fn my_group_ids(&self) -> Vec<String> {
        self.group_member.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(acls: &[&str], groups: &[(&str, bool)], streams: &[&str]) -> AccessContext {
        let acls: HashSet<String> = acls.iter().map(|s| s.to_string()).collect();
        let wildcard = acls.contains(ACL_SYSTEM_ADMIN);
        AccessContext {
            sub: "me".into(),
            acls,
            group_member: groups.iter().map(|(g, _)| g.to_string()).collect(),
            group_admin: groups
                .iter()
                .filter(|(_, a)| *a)
                .map(|(g, _)| g.to_string())
                .collect(),
            stream_ids: streams.iter().map(|s| s.to_string()).collect(),
            wildcard,
        }
    }

    #[test]
    fn has_acl_respects_wildcard() {
        let admin = ctx(&[ACL_SYSTEM_ADMIN], &[], &[]);
        assert!(admin.has_acl(ACL_MANAGE_USERS));
        assert!(admin.is_group_admin("anything"));
        assert!(admin.can_access_stream("anything"));

        let plain = ctx(&[ACL_MANAGE_SCIENCE_FILTERS], &[], &[]);
        assert!(plain.has_acl(ACL_MANAGE_SCIENCE_FILTERS));
        assert!(!plain.has_acl(ACL_MANAGE_USERS));
    }

    #[test]
    fn group_and_stream_membership() {
        let c = ctx(&[], &[("g1", true), ("g2", false)], &[STREAM_GCN_GRB]);
        assert!(c.in_group("g1") && c.in_group("g2"));
        assert!(c.is_group_admin("g1"));
        assert!(!c.is_group_admin("g2"));
        assert!(c.can_access_stream(STREAM_GCN_GRB));
        assert!(!c.can_access_stream(STREAM_GCN_FRB));
    }

    #[test]
    fn default_roles_and_streams_are_well_formed() {
        let roles = default_roles();
        assert_eq!(roles.len(), 4);
        let sa = roles.iter().find(|r| r.id == ROLE_SUPER_ADMIN).unwrap();
        assert!(sa.acls.contains(&ACL_SYSTEM_ADMIN.to_string()));
        assert!(roles.iter().all(|r| r.system));
        assert_eq!(default_streams().len(), 5);
    }

    #[test]
    fn instrument_stream_mapping() {
        assert_eq!(instrument_stream("Fermi-GBM-FIN"), Some(STREAM_GCN_GRB));
        assert_eq!(instrument_stream("Swift-BAT"), Some(STREAM_GCN_GRB));
        assert_eq!(instrument_stream("CHIME-FRB"), Some(STREAM_GCN_FRB));
        assert_eq!(instrument_stream("IceCube"), Some(STREAM_GCN_NEUTRINO));
        assert_eq!(instrument_stream("BOOM"), Some(STREAM_BOOM_OPTICAL));
        assert_eq!(instrument_stream("Mystery-Scope"), None);
    }
}
