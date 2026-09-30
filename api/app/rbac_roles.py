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
    "custom",
}

# Internal sentinel role for a user awaiting admin approval / activation.
# Users in this state have NO PostgreSQL database role (zero DB footprint).
PENDING_ROLE = "pending"

# ---------------------------------------------------------------------------
# sensorthings."User".status -- account lifecycle, an unconstrained
# VARCHAR(50). One of:
#   'pending'  -- registered (POST /Register or OIDC), awaiting an admin
#                 decision; not usable. Set alongside role='pending'.
#   'active'   -- approved, or created directly by an admin; usable.
#   'rejected' -- admin denied the request (role stays 'pending').
#   'deleted'  -- soft-deleted by DELETE /Users. A real DELETE is
#                 impossible: the AuditLog_actor_id_fkey ON DELETE SET NULL
#                 action runs as the AuditLog owner ('administrator'), which
#                 has no UPDATE on the append-only AuditLog, so it always
#                 fails. Deactivating in place is a plain UPDATE that leaves
#                 the row and its audit entries untouched.
# ---------------------------------------------------------------------------
PENDING_STATUS = "pending"
ACTIVE_STATUS = "active"
REJECTED_STATUS = "rejected"
DELETED_STATUS = "deleted"

# Maps each assignable RBAC role to its underlying PostgreSQL group role.
# Pending users are excluded — they receive no DB role until activated.
DB_ROLE_BY_RBAC_ROLE = {
    "viewer": "user",
    "editor": "user",
    "obs_manager": "sensor",
    "sensor": "sensor",
    "qc": "qc",
    "custom": "user",
}

# ---------------------------------------------------------------------------
# NOTE: viewer/editor/obs_manager/sensor/qc used to each need a CREATE POLICY
# call at approval time, dispatched to a stored function (viewer_policy(),
# editor_policy(), ...). Those per-user functions are dropped by
# 006_session_scoped_rls_policies.sql because istSOS users are not
# PostgreSQL roles, so a policy scoped ``TO <username>`` can never match a
# session that runs as a shared group role. Access for these roles is now
# enforced by static policies created once by that migration, scoped to the
# group role plus the app.current_user_id session claim set by set_role().
# Approving or activating a user into any assignable role is now a plain
# UPDATE of "User".role.
#
# 'custom' maps to the "user" group role. A custom user gets the standard
# viewer/editor read access from the static policies; any narrower,
# hand-specified rule is added separately via POST /Policies.
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
