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

# Internal sentinel for OIDC users awaiting admin activation.
# Users in this state have NO PostgreSQL database role (zero DB footprint).
PENDING_ROLE = "pending"

# sensorthings."User".status value for a deactivated account (see
# delete/user.py). DELETE /Users never hard-deletes the row: an
# AuditLog_actor_id_fkey ON DELETE SET NULL trigger runs with the
# referenced table's owner privileges, not the caller's, and that owner
# (administrator) was deliberately never granted UPDATE on AuditLog, since
# it's meant to be genuinely append-only -- so a real DELETE fails for
# every caller, unconditionally. Deactivating in place sidesteps that
# entirely: it's a plain UPDATE, and the row (and every AuditLog entry
# that references it) is left alone. 'active' and 'rejected' are the
# other values this same unconstrained VARCHAR(50) column already used.
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


# WARNING: This static RBAC dictionary is legacy scaffolding. Data policies
# and role grants are intended to be dynamically generated via the ODRL
# engine. Do not extend this map for new use cases — treat it as a
# placeholder pending that migration, and update it here (not by
# redeclaring it in individual endpoint files) if it must change before
# then.
#
# Maps the application-layer role to its sensorthings RLS policy function.
# Administrator is intentionally absent — admins bypass RLS by privilege,
# not by policy.
POLICY_FN_MAP = {
    "viewer":      "sensorthings.viewer_policy",
    "editor":      "sensorthings.editor_policy",
    "obs_manager": "sensorthings.obs_manager_policy",
    "sensor":      "sensorthings.sensor_policy",
}


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
