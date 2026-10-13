import os

import pytest


@pytest.fixture
def auth_headers(client):
    username = os.getenv("ISTSOS_ADMIN")
    password = os.getenv("ISTSOS_ADMIN_PASSWORD")

    if not username or not password:
        pytest.skip(
            "Set ISTSOS_ADMIN and ISTSOS_ADMIN_PASSWORD to run authenticated versioning tests."
        )

    response = client.post(
        "Login",
        data={"username": username, "password": password},
    )

    assert response.status_code == 200, f"Login done with status {response.status_code}"

    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
