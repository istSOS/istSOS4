"""Self-check: python test_eyeonwater2istsos.py (or pytest)."""

import requests

from eyeonwater2istsos import post_eyeonwater_to_istsos4

OBS = [
    {
        "id": 11,
        "user": {"user_n_code": "u9", "nickname": "Jo"},
        "device": {"device_model": "Pixel"},
        "lat": 46.1,
        "lon": 8.9,
        "image": {"date_photo": "2026-06-01T10:00:00Z"},
        "water": {"fu_value": 7, "p_ph": 7.5, "sd_depth": None, "other": 1},
    }
]


class FakeClient:
    """In-memory istSOS4: `field eq 'value'` filters, 409 on a repeated observation."""

    def __init__(self):
        self.rows, self.posts = {}, []

    def iter_list(self, entity, filter=None, top=None):
        field, value = filter.split(" eq ", 1)
        value = value.strip("'").replace("''", "'")
        rows = self.rows.get(entity.ENDPOINT, [])
        return (row for row in rows if row.serialize().get(field) == value)

    def post(self, entity, commit_message=None):
        rows = self.rows.setdefault(entity.ENDPOINT, [])
        payload = entity.serialize()
        if entity.ENDPOINT == "/Observations" and any(
            row.serialize() == payload for row in rows
        ):
            response = requests.Response()
            response.status_code = 409
            raise requests.HTTPError("409 Conflict", response=response)
        self.posts.append(entity.ENDPOINT)
        entity.iot_id = len(self.posts)
        rows.append(entity)


def test_rerun_reuses_entities_and_skips_duplicates():
    client = FakeClient()
    first = post_eyeonwater_to_istsos4(OBS, client, thing_id=3, network_name="eow")
    assert [row["key"] for row in first] == ["fu_value", "p_ph"]
    assert client.posts.count("/Observations") == 2

    posts_before = len(client.posts)
    again = post_eyeonwater_to_istsos4(OBS, client, thing_id=3, network_name="eow")
    assert again == []
    assert len(client.posts) == posts_before


if __name__ == "__main__":
    test_rerun_reuses_entities_and_skips_duplicates()
    print("ok")
