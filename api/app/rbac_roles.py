# ---------------------------------------------------------------------------
# Assignable application-layer roles.
# 'pending' is intentionally absent — it is an internal state, never directly
# assignable via the Policy/User API.
# ---------------------------------------------------------------------------
VALID_RBAC_ROLES = {
    "viewer",
    "editor",
    "obs_manager",
    "sensor",
    "qc",
    "odrl_governed",
}

# Internal sentinel for OIDC users awaiting admin activation.
# Users in this state have NO PostgreSQL database role (zero DB footprint).
PENDING_ROLE = "pending"

# Maps each assignable RBAC role to its underlying PostgreSQL group role.
# Pending users are excluded — they receive no DB role until activated.
DB_ROLE_BY_RBAC_ROLE = {
    "viewer": "user",
    "editor": "user",
    "obs_manager": "sensor",
    "sensor": "sensor",
    "qc": "qc",
    "odrl_governed": "user",
}

# ---------------------------------------------------------------------------
# HISTORICAL NOTE: viewer/editor/obs_manager/sensor/qc used to each need a
# CREATE POLICY call at approval time, dispatched through this map to a
# stored function (viewer_policy(), editor_policy(), ...). Those functions
# and that map are gone as of 007_session_scoped_rls_policies.sql: the
# per-approval policies they created were scoped ``TO <username>``, but no
# application code path has ever created an individual PostgreSQL login
# role for a real user, so those policies could never match any real
# session for any user, ever. Access control for these five roles is now
# enforced by static policies created once by that migration, scoped to
# the shared group role (see DB_ROLE_BY_RBAC_ROLE) plus a session claim
# (app.current_user_id, set by set_role() in v1/endpoints/functions.py) —
# approving or activating a user into one of these roles is now a plain
# UPDATE, nothing else.
#
# odrl_governed is the one role NOT covered by that migration — deferred
# for future ODRL work, per explicit scope decision. It still needs a
# dataset_id to mean anything, and update/admin_approval.py still calls
# sensorthings.odrl_governed_policy() directly, per-approval, exactly as
# before. create/user.py and activate_user.py still correctly skip policy
# creation for it entirely (it needs a dataset_id neither of those flows
# collects).
# ---------------------------------------------------------------------------


def validate_rbac_role(role: str) -> str:
    """Validate that *role* is one of the assignable RBAC roles.

    Raises ValueError for unknown roles, including the internal 'pending' state
    (which must never be set through the public API).
    """
    clean_role = role.strip().lower()
    if clean_role not in VALID_RBAC_ROLES:
        raise ValueError(
            "Invalid role. Supported roles are: "
            + ", ".join(sorted(VALID_RBAC_ROLES))
        )
    return clean_role


def get_db_role_for_rbac(role: str) -> str:
    """Return the PostgreSQL group role for a given RBAC role."""
    return DB_ROLE_BY_RBAC_ROLE[validate_rbac_role(role)]


# Sorted, JSON-serialisable view of the assignable roles, for OpenAPI
# schemas only (see app/models/role.py, app/models/approval_request.py).
# Derived from VALID_RBAC_ROLES rather than duplicated, so the documented
# enum can never drift from what validate_rbac_role() actually accepts.
ASSIGNABLE_ROLES = sorted(VALID_RBAC_ROLES)
