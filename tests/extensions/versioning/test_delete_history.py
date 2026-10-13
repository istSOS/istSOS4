from datetime import datetime, timedelta, timezone


def test_delete_is_visible_in_history(client, auth_headers, unique_name):
    start = datetime.now(timezone.utc) - timedelta(seconds=1)

    headers = {
        **auth_headers,
        "commit-message": "Create Thing for delete history test",
    }

    created = client.post(
        "Things",
        json={
            "name": unique_name("delete-history"),
            "description": "Temporary Thing for delete history regression test",
        },
        headers=headers,
    )
    assert created.status_code == 201, created.text

    thing_url = client.location_of(created)
    deleted = False

    try:
        headers["commit-message"] = "Delete Thing for history test"

        response = client.delete(thing_url, headers=headers)
        assert response.status_code == 200, response.text
        deleted = True

        end = datetime.now(timezone.utc) + timedelta(seconds=1)

        response = client.get(
            thing_url,
            params={
                "$from_to": f"{start.isoformat()}/{end.isoformat()}",
                "$expand": "Commit",
            },
            headers=auth_headers,
        )
        assert response.status_code == 200, response.text

        versions = response.json()["value"]
        delete_versions = [
            version
            for version in versions
            if version["Commit"]["actionType"] == "DELETE"
        ]

        assert len(delete_versions) == 1, versions

        validity = delete_versions[0]["systemTimeValidity"]
        boundaries = validity.split("/")

        assert len(boundaries) == 2, validity
        assert boundaries[0] and boundaries[1], validity
        assert boundaries[0] == boundaries[1], validity

        deletion_time = boundaries[0]

        response = client.get(
            thing_url,
            params={"$as_of": deletion_time, "$expand": "Commit"},
            headers=auth_headers,
        )
        assert response.status_code == 200, response.text

        version_at_deletion = response.json()

        assert (
            version_at_deletion["Commit"]["@iot.id"]
            == delete_versions[0]["Commit"]["@iot.id"]
        ), response.text

    finally:
        if not deleted:
            headers["commit-message"] = "Clean up delete history test"
            cleanup = client.delete(thing_url, headers=headers)
            assert cleanup.status_code in (200, 404), cleanup.text
