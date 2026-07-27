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
    "custom",
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
    # Custom policies still require baseline schema/table permissions.
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
