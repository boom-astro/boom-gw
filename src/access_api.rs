//! HTTP handlers for the access-control surface: the enriched
//! `/api/users/me`, the user roster + role assignment, and the
//! read-only roles/ACLs catalog. Groups and streams (S3) live here too.
//!
//! These reuse the response helpers and the [`crate::api::access_ctx`]
//! authorization helper from [`crate::api`]; routes are registered in
//! `crate::api::configure`.

use actix_web::{web, HttpRequest, HttpResponse};
use futures::TryStreamExt;
use mongodb::bson::doc;
use serde::Deserialize;
use serde_json::json;

use crate::access::{self, AccessContext};
use crate::api::{access_ctx, bad_request, internal_error, not_found, ok, upsert_response};
use crate::archive::{Archive, GroupDoc, GroupStreamDoc, GroupUserDoc, StreamDoc, StreamUserDoc};
use crate::auth::{forbidden, AuthConfig};

/// `{id, name}` reference used in the enriched profile payload.
fn stream_ref(s: &StreamDoc) -> serde_json::Value {
    json!({ "id": s.id, "name": s.name })
}

/// Resolve the [`StreamDoc`]s for a set of stream ids, as `{id,name}`
/// refs sorted by name.
async fn stream_refs(
    archive: &Archive,
    ids: &[String],
) -> Result<Vec<serde_json::Value>, HttpResponse> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let mut cursor = archive
        .streams()
        .find(doc! {"_id": {"$in": ids}})
        .await
        .map_err(internal_error)?;
    let mut out = Vec::new();
    while let Some(s) = cursor.try_next().await.map_err(internal_error)? {
        out.push(s);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out.iter().map(stream_ref).collect())
}

/// The streams a given group can access, as `{id,name}` refs.
async fn group_stream_refs(
    archive: &Archive,
    group_id: &str,
) -> Result<Vec<serde_json::Value>, HttpResponse> {
    let mut cursor = archive
        .group_streams()
        .find(doc! {"group_id": group_id})
        .await
        .map_err(internal_error)?;
    let mut ids = Vec::new();
    while let Some(gs) = cursor.try_next().await.map_err(internal_error)? {
        ids.push(gs.stream_id);
    }
    stream_refs(archive, &ids).await
}

/// `GET /api/users/me` — the SPA's source of truth: identity plus
/// effective ACLs, roles, group memberships (with admin flag + each
/// group's streams), and accessible streams.
pub async fn get_my_profile(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let user = match archive.users().find_one(doc! {"_id": &ctx.sub}).await {
        Ok(Some(u)) => u,
        Ok(None) => return not_found("user"),
        Err(e) => return internal_error(e),
    };

    // Group refs (name + admin flag + the group's streams).
    let mut groups = Vec::new();
    if !ctx.group_member.is_empty() {
        let gids: Vec<&String> = ctx.group_member.iter().collect();
        let mut cursor = match archive.groups().find(doc! {"_id": {"$in": &gids}}).await {
            Ok(c) => c,
            Err(e) => return internal_error(e),
        };
        let mut docs: Vec<GroupDoc> = Vec::new();
        loop {
            match cursor.try_next().await {
                Ok(Some(g)) => docs.push(g),
                Ok(None) => break,
                Err(e) => return internal_error(e),
            }
        }
        docs.sort_by(|a, b| a.name.cmp(&b.name));
        for g in docs {
            let streams = match group_stream_refs(&archive, &g.id).await {
                Ok(s) => s,
                Err(resp) => return resp,
            };
            groups.push(json!({
                "id": g.id,
                "name": g.name,
                "admin": ctx.group_admin.contains(&g.id),
                "streams": streams,
            }));
        }
    }

    let stream_ids: Vec<String> = ctx.stream_ids.iter().cloned().collect();
    let streams = match stream_refs(&archive, &stream_ids).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    let mut acls: Vec<String> = ctx.acls.iter().cloned().collect();
    acls.sort();

    ok(json!({
        "sub": user.sub,
        "email": user.email,
        "display_name": user.display_name,
        "acls": acls,
        "roles": user.role_ids,
        "groups": groups,
        "streams": streams,
    }))
}

/// `GET /api/users` — a minimal roster (`sub`, `display_name`, `email`)
/// for member/role pickers. Available to any signed-in user; full
/// management is gated on `Manage users` via [`patch_user`].
pub async fn list_users(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    if let Err(resp) = access_ctx(&req, &archive, &auth).await {
        return resp;
    }
    let mut cursor = match archive.users().find(doc! {}).await {
        Ok(c) => c,
        Err(e) => return internal_error(e),
    };
    let mut out = Vec::new();
    loop {
        match cursor.try_next().await {
            Ok(Some(u)) => out.push(json!({
                "sub": u.sub,
                "display_name": u.display_name,
                "email": u.email,
                "roles": u.role_ids,
            })),
            Ok(None) => break,
            Err(e) => return internal_error(e),
        }
    }
    ok(out)
}

#[derive(Debug, Deserialize)]
pub struct PatchUserBody {
    /// Replacement role set (role slugs). Must all be known roles.
    role_ids: Vec<String>,
}

/// `PATCH /api/users/{sub}` — set a user's roles. Gated on
/// `Manage users`. Refuses to strip the last Super admin in the system.
pub async fn patch_user(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
    body: web::Json<PatchUserBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if !ctx.has_acl(access::ACL_MANAGE_USERS) {
        return crate::auth::forbidden("requires the Manage users ACL");
    }
    let sub = path.into_inner();
    let body = body.into_inner();

    // Validate role slugs against the catalog.
    let known: std::collections::HashSet<String> =
        access::default_roles().into_iter().map(|r| r.id).collect();
    if let Some(bad) = body.role_ids.iter().find(|r| !known.contains(*r)) {
        return bad_request(format!("unknown role: {bad}"));
    }

    // Lockout guard: don't remove the last Super admin.
    let removing_super = !body.role_ids.iter().any(|r| r == access::ROLE_SUPER_ADMIN);
    if removing_super {
        let was_super = matches!(
            archive.users().find_one(doc! {"_id": &sub}).await,
            Ok(Some(ref u)) if u.role_ids.iter().any(|r| r == access::ROLE_SUPER_ADMIN)
        );
        if was_super {
            let supers = match archive
                .users()
                .count_documents(doc! {"role_ids": access::ROLE_SUPER_ADMIN})
                .await
            {
                Ok(n) => n,
                Err(e) => return internal_error(e),
            };
            if supers <= 1 {
                return bad_request("cannot remove the last Super admin");
            }
        }
    }

    match archive.set_user_roles(&sub, &body.role_ids).await {
        Ok(true) => match archive.users().find_one(doc! {"_id": &sub}).await {
            Ok(Some(u)) => ok(u),
            Ok(None) => not_found("user"),
            Err(e) => internal_error(e),
        },
        Ok(false) => not_found("user"),
        Err(e) => internal_error(e),
    }
}

/// `GET /api/roles` — the role catalog (seeded defaults + any custom).
pub async fn list_roles(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    if let Err(resp) = access_ctx(&req, &archive, &auth).await {
        return resp;
    }
    match crate::api::collect::<crate::archive::RoleDoc>(
        &archive,
        crate::archive::ROLES_COLLECTION,
        doc! {},
        mongodb::options::FindOptions::builder()
            .sort(doc! {"name": 1})
            .build(),
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

/// `GET /api/acls` — the full ACL vocabulary (code-defined).
pub async fn list_acls(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    if let Err(resp) = access_ctx(&req, &archive, &auth).await {
        return resp;
    }
    ok(access::ALL_ACLS.to_vec())
}

// --- Groups ----------------------------------------------------------------

/// May the caller manage this group (edit/delete it, manage its
/// members/streams)? True for group admins and `Manage groups` holders.
fn can_manage_group(ctx: &AccessContext, group_id: &str) -> bool {
    ctx.is_group_admin(group_id) || ctx.has_acl(access::ACL_MANAGE_GROUPS)
}

/// JSON view of a group with the caller's admin flag.
fn group_json(g: &GroupDoc, admin: bool) -> serde_json::Value {
    json!({
        "id": g.id,
        "name": g.name,
        "description": g.description,
        "admin": admin,
    })
}

/// `GET /api/groups` — groups the caller belongs to, or all groups with
/// `Manage groups`. Each carries the caller's `admin` flag.
pub async fn list_groups(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let query = if ctx.has_acl(access::ACL_MANAGE_GROUPS) {
        doc! {}
    } else {
        let ids: Vec<&String> = ctx.group_member.iter().collect();
        doc! {"_id": {"$in": ids}}
    };
    let mut cursor = match archive.groups().find(query).await {
        Ok(c) => c,
        Err(e) => return internal_error(e),
    };
    let mut out = Vec::new();
    loop {
        match cursor.try_next().await {
            Ok(Some(g)) => {
                let admin = ctx.is_group_admin(&g.id);
                out.push(group_json(&g, admin));
            }
            Ok(None) => break,
            Err(e) => return internal_error(e),
        }
    }
    out.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    ok(out)
}

#[derive(Debug, Deserialize)]
pub struct CreateGroupBody {
    name: String,
    #[serde(default)]
    description: String,
}

/// `POST /api/groups` — create a group; the creator becomes its admin.
/// Gated on `Manage groups`.
pub async fn create_group(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    body: web::Json<CreateGroupBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if !ctx.has_acl(access::ACL_MANAGE_GROUPS) {
        return forbidden("requires the Manage groups ACL");
    }
    let body = body.into_inner();
    if body.name.trim().is_empty() {
        return bad_request("group name is required");
    }
    let group = GroupDoc::new(body.name.trim(), body.description);
    if let Err(e) = archive.groups().insert_one(&group).await {
        // Likely a duplicate name (unique index).
        return bad_request(format!("could not create group: {e}"));
    }
    // Creator becomes the group's first admin.
    let membership = GroupUserDoc::new(&group.id, &ctx.sub, true);
    if let Err(e) = archive.group_users().insert_one(&membership).await {
        return internal_error(e);
    }
    upsert_response(true, group_json(&group, true))
}

/// `GET /api/groups/{id}` — group detail with members and streams.
/// Visible to members or `Manage groups`.
pub async fn get_group(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let gid = path.into_inner();
    if !ctx.in_group(&gid) && !ctx.has_acl(access::ACL_MANAGE_GROUPS) {
        return not_found("group");
    }
    group_detail(&archive, &ctx, &gid).await
}

/// Build the full group-detail response (group + members joined with
/// display names + streams). Shared by `get_group` and the member /
/// stream mutation handlers so they can echo the refreshed group.
async fn group_detail(archive: &Archive, ctx: &AccessContext, gid: &str) -> HttpResponse {
    let group = match archive.groups().find_one(doc! {"_id": gid}).await {
        Ok(Some(g)) => g,
        Ok(None) => return not_found("group"),
        Err(e) => return internal_error(e),
    };

    let mut rows: Vec<GroupUserDoc> = Vec::new();
    let mut cursor = match archive.group_users().find(doc! {"group_id": gid}).await {
        Ok(c) => c,
        Err(e) => return internal_error(e),
    };
    loop {
        match cursor.try_next().await {
            Ok(Some(gu)) => rows.push(gu),
            Ok(None) => break,
            Err(e) => return internal_error(e),
        }
    }
    let mut members = Vec::new();
    for gu in &rows {
        let user = archive.users().find_one(doc! {"_id": &gu.user_sub}).await;
        let (display_name, email) = match user {
            Ok(Some(u)) => (u.display_name, u.email),
            _ => (None, None),
        };
        members.push(json!({
            "sub": gu.user_sub,
            "admin": gu.admin,
            "display_name": display_name,
            "email": email,
        }));
    }

    let streams = match group_stream_refs(archive, gid).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };

    ok(json!({
        "id": group.id,
        "name": group.name,
        "description": group.description,
        "admin": ctx.is_group_admin(gid),
        "members": members,
        "streams": streams,
    }))
}

#[derive(Debug, Deserialize)]
pub struct PatchGroupBody {
    name: Option<String>,
    description: Option<String>,
}

/// `PATCH /api/groups/{id}` — rename / re-describe. Group admins or
/// `Manage groups`.
pub async fn patch_group(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
    body: web::Json<PatchGroupBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let gid = path.into_inner();
    if !can_manage_group(&ctx, &gid) {
        return forbidden("requires group admin or the Manage groups ACL");
    }
    let mut set = doc! {};
    if let Some(name) = &body.name {
        set.insert("name", name.trim());
    }
    if let Some(desc) = &body.description {
        set.insert("description", desc);
    }
    if set.is_empty() {
        return bad_request("nothing to update");
    }
    match archive
        .groups()
        .update_one(doc! {"_id": &gid}, doc! {"$set": set})
        .await
    {
        Ok(res) if res.matched_count == 1 => group_detail(&archive, &ctx, &gid).await,
        Ok(_) => not_found("group"),
        Err(e) => bad_request(format!("could not update group: {e}")),
    }
}

/// `DELETE /api/groups/{id}` — delete the group and its join rows.
pub async fn delete_group(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let gid = path.into_inner();
    if !can_manage_group(&ctx, &gid) {
        return forbidden("requires group admin or the Manage groups ACL");
    }
    match archive.delete_group_cascade(&gid).await {
        Ok(true) => ok(json!({"deleted": gid})),
        Ok(false) => not_found("group"),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct AddMemberBody {
    sub: String,
    #[serde(default)]
    admin: bool,
}

/// `POST /api/groups/{id}/members` — add (or update the admin flag of) a
/// member. Idempotent. Auto-grants the group's streams to the new
/// member (SkyPortal "accessible via group streams").
pub async fn add_group_member(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
    body: web::Json<AddMemberBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let gid = path.into_inner();
    if !can_manage_group(&ctx, &gid) {
        return forbidden("requires group admin or the Manage groups ACL");
    }
    if archive
        .groups()
        .find_one(doc! {"_id": &gid})
        .await
        .ok()
        .flatten()
        .is_none()
    {
        return not_found("group");
    }
    let body = body.into_inner();
    let membership = GroupUserDoc::new(&gid, &body.sub, body.admin);
    if let Err(e) = archive
        .group_users()
        .update_one(
            doc! {"_id": {"group_id": &gid, "user_sub": &body.sub}},
            doc! {"$set": {
                "group_id": &gid,
                "user_sub": &body.sub,
                "admin": body.admin,
                "created_at": membership.created_at,
            }},
        )
        .upsert(true)
        .await
    {
        return internal_error(e);
    }
    // Grant the group's streams to the new member.
    let stream_ids = match archive.group_stream_ids(&gid).await {
        Ok(s) => s,
        Err(e) => return internal_error(e),
    };
    for sid in stream_ids {
        let su = StreamUserDoc::new(&sid, &body.sub);
        if let Err(e) = archive
            .stream_users()
            .update_one(
                doc! {"_id": {"stream_id": &sid, "user_sub": &body.sub}},
                doc! {"$setOnInsert": mongodb::bson::to_document(&su).unwrap_or_default()},
            )
            .upsert(true)
            .await
        {
            return internal_error(e);
        }
    }
    group_detail(&archive, &ctx, &gid).await
}

#[derive(Debug, Deserialize)]
pub struct AddStreamBody {
    stream_id: String,
}

/// `DELETE /api/groups/{id}/members/{sub}` — remove a member. Refuses to
/// remove the last admin (lockout guard).
pub async fn remove_group_member(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let (gid, sub) = path.into_inner();
    if !can_manage_group(&ctx, &gid) {
        return forbidden("requires group admin or the Manage groups ACL");
    }
    // Lockout guard: don't remove the group's last admin.
    let target = archive
        .group_users()
        .find_one(doc! {"_id": {"group_id": &gid, "user_sub": &sub}})
        .await;
    if let Ok(Some(gu)) = target {
        if gu.admin {
            match archive.group_admin_count(&gid).await {
                Ok(n) if n <= 1 => return bad_request("cannot remove the group's last admin"),
                Err(e) => return internal_error(e),
                _ => {}
            }
        }
    }
    if let Err(e) = archive
        .group_users()
        .delete_one(doc! {"_id": {"group_id": &gid, "user_sub": &sub}})
        .await
    {
        return internal_error(e);
    }
    group_detail(&archive, &ctx, &gid).await
}

/// `POST /api/groups/{id}/streams` — give the group access to a stream.
/// Group admins (or `Manage streams`).
pub async fn add_group_stream(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
    body: web::Json<AddStreamBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let gid = path.into_inner();
    if !can_manage_group(&ctx, &gid) && !ctx.has_acl(access::ACL_MANAGE_STREAMS) {
        return forbidden("requires group admin or the Manage streams ACL");
    }
    let body = body.into_inner();
    if archive
        .streams()
        .find_one(doc! {"_id": &body.stream_id})
        .await
        .ok()
        .flatten()
        .is_none()
    {
        return not_found("stream");
    }
    let gs = GroupStreamDoc::new(&gid, &body.stream_id);
    if let Err(e) = archive
        .group_streams()
        .update_one(
            doc! {"_id": {"group_id": &gid, "stream_id": &body.stream_id}},
            doc! {"$setOnInsert": mongodb::bson::to_document(&gs).unwrap_or_default()},
        )
        .upsert(true)
        .await
    {
        return internal_error(e);
    }
    group_detail(&archive, &ctx, &gid).await
}

/// `DELETE /api/groups/{id}/streams/{stream_id}` — revoke a group's
/// access to a stream.
pub async fn remove_group_stream(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let (gid, sid) = path.into_inner();
    if !can_manage_group(&ctx, &gid) && !ctx.has_acl(access::ACL_MANAGE_STREAMS) {
        return forbidden("requires group admin or the Manage streams ACL");
    }
    if let Err(e) = archive
        .group_streams()
        .delete_one(doc! {"_id": {"group_id": &gid, "stream_id": &sid}})
        .await
    {
        return internal_error(e);
    }
    group_detail(&archive, &ctx, &gid).await
}

// --- Streams ---------------------------------------------------------------

/// `GET /api/streams` — the stream catalog. Any signed-in user.
pub async fn list_streams(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
) -> HttpResponse {
    if let Err(resp) = access_ctx(&req, &archive, &auth).await {
        return resp;
    }
    match crate::api::collect::<StreamDoc>(
        &archive,
        crate::archive::STREAMS_COLLECTION,
        doc! {},
        mongodb::options::FindOptions::builder()
            .sort(doc! {"name": 1})
            .build(),
    )
    .await
    {
        Ok(items) => ok(items),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateStreamBody {
    id: String,
    name: String,
    #[serde(default)]
    description: String,
}

/// `POST /api/streams` — define a stream. Gated on `Manage streams`.
pub async fn create_stream(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    body: web::Json<CreateStreamBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if !ctx.has_acl(access::ACL_MANAGE_STREAMS) {
        return forbidden("requires the Manage streams ACL");
    }
    let body = body.into_inner();
    if body.id.trim().is_empty() || body.name.trim().is_empty() {
        return bad_request("stream id and name are required");
    }
    let stream = StreamDoc {
        id: body.id.trim().to_string(),
        name: body.name.trim().to_string(),
        description: body.description,
        system: false,
    };
    match archive
        .streams()
        .update_one(
            doc! {"_id": &stream.id},
            doc! {"$set": mongodb::bson::to_document(&stream).unwrap_or_default()},
        )
        .upsert(true)
        .await
    {
        Ok(res) => upsert_response(res.upserted_id.is_some(), stream),
        Err(e) => internal_error(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct GrantStreamBody {
    sub: String,
}

/// `POST /api/streams/{id}/users` — grant a user direct stream access.
/// Gated on `Manage streams`.
pub async fn grant_stream_user(
    req: HttpRequest,
    archive: web::Data<Archive>,
    auth: web::Data<AuthConfig>,
    path: web::Path<String>,
    body: web::Json<GrantStreamBody>,
) -> HttpResponse {
    let ctx = match access_ctx(&req, &archive, &auth).await {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    if !ctx.has_acl(access::ACL_MANAGE_STREAMS) {
        return forbidden("requires the Manage streams ACL");
    }
    let sid = path.into_inner();
    let body = body.into_inner();
    if archive
        .streams()
        .find_one(doc! {"_id": &sid})
        .await
        .ok()
        .flatten()
        .is_none()
    {
        return not_found("stream");
    }
    let su = StreamUserDoc::new(&sid, &body.sub);
    match archive
        .stream_users()
        .update_one(
            doc! {"_id": {"stream_id": &sid, "user_sub": &body.sub}},
            doc! {"$setOnInsert": mongodb::bson::to_document(&su).unwrap_or_default()},
        )
        .upsert(true)
        .await
    {
        Ok(_) => ok(json!({"stream_id": sid, "user_sub": body.sub})),
        Err(e) => internal_error(e),
    }
}
