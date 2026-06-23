import argparse
import json
import os
from datetime import datetime
from typing import Iterable

import requests
from eyeonwater2istsos import post_eyeonwater_to_istsos4
from models import (
    build_istsos_url,
    get_credentials,
    load_environment,
    login,
)

EYEONWATER_API_URL = "https://www.eyeonwater.org/api/observations"


def parse_bbox(value):
    if value is None:
        return None

    if isinstance(value, str):
        bbox = [part.strip() for part in value.split(",")]
    else:
        bbox = list(value)

    if len(bbox) != 4:
        raise ValueError("bbox must be [min_lat, min_lon, max_lat, max_lon]")

    return [float(part) for part in bbox]


def get_eyeonwater_observations(
    begin: str | datetime | None = None,
    end: str | datetime | None = None,
    bbox: Iterable[float] | str | None = None,
    timeout: int = 30,
) -> dict | list:
    """
    Request EyeOnWater observations and return JSON.

    bbox format:
        [min_lat, min_lon, max_lat, max_lon]

    begin/end:
        ISO strings or datetime objects, e.g. "2026-01-01T00:00:00Z"
    """

    def fmt_time(t):
        if isinstance(t, datetime):
            return t.isoformat()
        return t

    params = {}

    if begin is not None:
        params["begin"] = fmt_time(begin)

    if end is not None:
        params["end"] = fmt_time(end)

    bbox = parse_bbox(bbox)
    if bbox is not None:
        params["bbox"] = ",".join(map(str, bbox))
        params["bboxVersion"] = (
            "1.3.0"  # WMS 1.3.0 uses latitude/longitude axis ordering.
        )

    r = requests.get(EYEONWATER_API_URL, params=params, timeout=timeout)

    r.raise_for_status()
    return r.json()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Import EyeOnWater observations from API into istSOS4."
    )
    parser.add_argument(
        "--thing-id",
        type=int,
        default=os.getenv("EYEONWATER_THING_ID"),
        help="Thing @iot.id to associate with the imported datastreams.",
    )
    parser.add_argument(
        "--network-name",
        default=os.getenv("EYEONWATER_NETWORK_NAME"),
        help="Network name to create or reuse for the imported datastreams.",
    )
    parser.add_argument(
        "--commit-message",
        default="Import EyeOnWater observations",
        help="Commit message sent to istSOS4.",
    )
    parser.add_argument(
        "--begin",
        default=os.getenv("EYEONWATER_BEGIN"),
        help="Start time for fetching EyeOnWater observations (ISO string or datetime).",
    )
    parser.add_argument(
        "--end",
        default=os.getenv("EYEONWATER_END"),
        help="End time for fetching EyeOnWater observations (ISO string or datetime).",
    )
    parser.add_argument(
        "--bbox",
        default=os.getenv("EYEONWATER_BBOX"),
        help="Bounding box for fetching EyeOnWater observations (min_lat,min_lon,max_lat,max_lon).",
    )
    return parser.parse_args()


def main():
    load_environment()
    args = parse_args()

    if args.thing_id is None:
        raise RuntimeError(
            "Missing Thing id. Pass --thing-id or set EYEONWATER_THING_ID in .env."
        )

    if not args.network_name:
        raise RuntimeError(
            "Missing Network name. Pass --network-name or set "
            "EYEONWATER_NETWORK_NAME in .env."
        )

    server_url = build_istsos_url()
    username, password = get_credentials()
    token = login(server_url, username, password)
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "commit-message": args.commit_message,
    }

    data = get_eyeonwater_observations(
        begin=args.begin,
        end=args.end,
        bbox=args.bbox,
    )
    posted = post_eyeonwater_to_istsos4(
        eyeonwater_json=data,
        sta_endpoint=server_url,
        thing_id=args.thing_id,
        network_name=args.network_name,
        headers=headers,
    )

    print(json.dumps(posted, ensure_ascii=False, indent=2))
    print(f"Imported {len(posted)} observations into {server_url}.")


if __name__ == "__main__":
    main()
