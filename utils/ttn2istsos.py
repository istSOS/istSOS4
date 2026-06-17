import argparse
import hashlib
import json
import os
from pathlib import Path

import requests

from xlsx2istsos import (
    build_istsos_url,
    get_credentials,
    load_environment,
    login,
)


class TTNClient:
    def __init__(self, base_url, headers=None):
        self.base_url = base_url.rstrip("/")
        self.headers = headers

    def get(self, endpoint, params=None):
        r = requests.get(
            f"{self.base_url}/{endpoint}", params=params, headers=self.headers
        )
        if not r.ok:
            message = r.text.strip()
            raise RuntimeError(
                f"GET {endpoint} failed with status {r.status_code}: {message}"
            )
        data = r.json()
        return data.get("value", data)


def main():
    load_environment()

    server_url = build_istsos_url()
    username, password = get_credentials()
    token = login(server_url, username, password)
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "commit-message": "Import from The Things Network",
    }
    client = TTNClient(
        "https://eu1.cloud.thethings.network/api/v3/search/applications", None
    )
    client.get()
    return


if __name__ == "__main__":
    main()
