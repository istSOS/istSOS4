"""Authentication configuration validation helpers."""

MIN_SECRET_KEY_LENGTH = 32
PLACEHOLDER_SECRET_KEYS = {
    "admin",
    "changeme",
    "default",
    "password",
    "secret",
    "your-secret-key",
}


def validate_auth_config(
    authorization: int,
    secret_key: str | None,
    debug: int,
) -> list[str]:
    """Validate authentication settings and return non-fatal warnings."""
    warnings = []

    if authorization:
        if secret_key is None or not secret_key.strip():
            raise ValueError(
                "AUTHORIZATION=1 requires SECRET_KEY to be set to a strong value."
            )

        clean_secret = secret_key.strip()
        if clean_secret.lower() in PLACEHOLDER_SECRET_KEYS:
            raise ValueError(
                "SECRET_KEY uses a placeholder value. "
                "Set a unique random value generated from a secure source."
            )

        if len(clean_secret) < MIN_SECRET_KEY_LENGTH:
            raise ValueError(
                "SECRET_KEY is too short. "
                f"Use at least {MIN_SECRET_KEY_LENGTH} characters."
            )
    else:
        warnings.append(
            "AUTHORIZATION=0 disables authentication and authorization checks. "
            "Use only in controlled environments and set AUTHORIZATION=1 for "
            "secure deployments."
        )
        if not debug:
            warnings.append(
                "DEBUG=0 with AUTHORIZATION=0 means the API is running in an "
                "unsafe configuration."
            )

    return warnings