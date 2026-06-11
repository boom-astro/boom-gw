# 5. Access control: users, groups, streams, roles

boom-gw uses a SkyPortal-style access model. This tutorial walks
through it end to end: the concepts, then a concrete scenario where
you create a group, add a member, grant it streams, share a filter,
and watch ACLs gate what each user can do.

Prerequisite: the stack from [tutorial 1](01-getting-started.md),
signed in as `cough052@ligo.org` (a **Super admin**).

## The four concepts

- **Users** — persisted, keyed by the OIDC `sub`. Created
  just-in-time on first sign-in (no pre-registration). In dev you
  "become" any user by dev-logging-in as that sub.
- **Roles** — named bundles of **ACLs**. Four are seeded:

  | Role | ACLs |
  |------|------|
  | **Super admin** | `System admin` (a wildcard — passes every check) |
  | **Group admin** | `Manage groups`, `Manage science filters`, `Publish alerts`, `Upload data` |
  | **Full user** | `Manage science filters`, `Upload data` |
  | **View only** | (none) |

  A user's effective ACLs are the union over their roles.
- **Groups** — collaborations. Data (science filters today, more
  later) is shared *with a group*; membership is the unit of
  visibility. Each group has **admins** who manage its membership and
  streams.
- **Streams** — the five messenger ingest channels: `gracedb_gw`,
  `gcn_grb`, `gcn_frb`, `gcn_neutrino`, `boom_optical`. A group is
  granted access to streams; a filter can only draw from its group's
  streams; a user sees a stream's cross-matches only if they can
  access it.

## Who am I?

Your effective identity lives at `GET /api/users/me` — the SPA's
source of truth for nav gating and the filter pickers:

```sh
gw http://127.0.0.1:8080/api/users/me \
  | jq '.data | {sub, acls, roles, groups:[.groups[].name], streams:[.streams[].id]}'
```

As `cough052@ligo.org` you'll see `acls: ["System admin"]`, role
`super_admin`, membership in **MMA team**, and all five streams.

The catalog endpoints:

```sh
gw http://127.0.0.1:8080/api/roles  | jq '.data[] | {id:._id, acls}'
gw http://127.0.0.1:8080/api/acls   | jq .data
gw http://127.0.0.1:8080/api/streams| jq '.data[] | {id:._id, name}'
```

## The bootstrap (how anyone gets in)

There's a chicken-and-egg problem: someone has to be the first admin.
boom-gw resolves it two ways, both via the `BOOM_GW_SITE_ADMINS` env
var (`make run` sets it to `load-demo-data,cough052@ligo.org`):

- any `sub` listed there gets **Super admin** on sign-in;
- if `BOOM_GW_SITE_ADMINS` is empty **and** there are no users yet,
  the very first person to sign in becomes Super admin (so a fresh
  deployment isn't locked out).

Everyone else starts as a **Full user**.

## Scenario: a new group with a restricted member

Let's create a "Kilonova hunters" group, give it only the optical and
GRB streams, add a teammate, and share a filter — then sign in as the
teammate to see the effect.

### 1. Create the group (UI)

Click **Groups → New group**, name it `Kilonova hunters`, **Create**.
You land on its detail page as its first admin. (Group creation needs
the `Manage groups` ACL, which Super admins and Group admins have.)

API equivalent:

```sh
GID=$(gw -X POST http://127.0.0.1:8080/api/groups \
  -H 'Content-Type: application/json' \
  -d '{"name":"Kilonova hunters","description":"EM counterparts"}' \
  | jq -r '.data.id')
```

### 2. Grant it streams

On the group page, under **Streams**, add `BOOM optical` and
`GCN GRB`. Deliberately leave out neutrino/FRB.

```sh
for s in boom_optical gcn_grb; do
  gw -X POST http://127.0.0.1:8080/api/groups/$GID/streams \
     -H 'Content-Type: application/json' -d "{\"stream_id\":\"$s\"}"
done
```

### 3. Add a member

Under **Members**, use the picker to add `bob@ligo.org` (not an admin).
Adding a member **auto-grants them the group's streams** — so Bob can
now access optical + GRB data through this group.

```sh
gw -X POST http://127.0.0.1:8080/api/groups/$GID/members \
   -H 'Content-Type: application/json' \
   -d '{"sub":"bob@ligo.org","admin":false}'
```

> Try removing yourself while you're the only admin — boom-gw refuses
> (the last-admin lockout guard). Same for stripping the last Super
> admin via the Users page.

### 4. Share a filter with the group

Go to **Science filters → New filter**, name it, pick **Kilonova
hunters** as the group, and (now that the group has streams) select
**BOOM optical** in the stream multi-select. Add a loose cut like
`spatial_overlap_min = 0.1`. **Create**.

### 5. See it as the member

Open a private/incognito window (or log out), sign in as
`bob@ligo.org`, and notice:

- The nav has **no** *Users* or *Streams* admin links — Bob is a Full
  user without those ACLs.
- **Groups** shows *Kilonova hunters* (Bob is a member, not an admin —
  he can't edit it).
- **Science filters** shows the filter you shared — and *not* the
  demo's MMA-team filters, because Bob isn't in MMA team.
- On a superevent's Cross-matches tab, applying Bob's filter shows
  only optical/GRB matches; neutrino and FRB rows are stream-gated out
  because Bob's group can't access those streams.

Deep-linking Bob to `/admin/users` bounces him back to `/superevents`
— the route is ACL-guarded, not just the nav link.

## Roles in practice

As Super admin, **Users** (admin nav) lists every provisioned user
with a role multi-select. Assign `bob@ligo.org` the **Group admin**
role and he gains the ability to create his own groups and publish
alerts. Assigning roles needs the `Manage users` ACL.

```sh
gw -X PATCH http://127.0.0.1:8080/api/users/bob@ligo.org \
  -H 'Content-Type: application/json' -d '{"role_ids":["group_admin"]}'
```

## What gates what (cheat sheet)

| Action | Requirement |
|--------|-------------|
| See a science filter | owner, or member of its group, or `Manage science filters` |
| Edit/delete a filter | owner, group **admin**, or `Manage science filters` |
| Create a group | `Manage groups` |
| Manage a group's members/streams | that group's **admin**, or `Manage groups` |
| Assign user roles | `Manage users` |
| Create streams / grant stream access | `Manage streams` |
| Publish a public alert | `Publish alerts` |
| See a stream's cross-matches | stream access (direct grant or via a group) |

`System admin` (Super admin's ACL) is a wildcard that satisfies all of
the above.

---

Next: [Ingesting external alerts](06-ingesting-alerts.md).
</content>
