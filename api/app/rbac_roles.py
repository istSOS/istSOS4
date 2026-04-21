VALID_RBAC_ROLES = {
    "viewer",
    "editor",
    "obs_manager",
    "sensor",
    "custom",
}

DENIED_CREATOR_ROLES = {"viewer"}

DB_ROLE_BY_RBAC_ROLE = {
    "viewer": "user",
    "editor": "user",
    "obs_manager": "sensor",
    "sensor": "sensor",
    "custom": "user",
}


def validate_rbac_role(role: str) -> str:
    clean_role = role.strip().lower()
    if clean_role not in VALID_RBAC_ROLES:
        raise ValueError(
            "Invalid role. Supported roles are: "
            + ", ".join(sorted(VALID_RBAC_ROLES))
        )
    return clean_role


def get_db_role_for_rbac(role: str) -> str:
    return DB_ROLE_BY_RBAC_ROLE[validate_rbac_role(role)]


def check_create_permission(role) -> bool:
    if role is None:
        return True
    return role.lower() not in DENIED_CREATOR_ROLES
