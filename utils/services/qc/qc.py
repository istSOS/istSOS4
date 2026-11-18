"""QC updater: every INTERVAL_MINUTES, qc_historical's check on what is new in
each datastream with constraints. qc_historical is for entire series; this
keeps them up to date.

New means a resultTime after the previous run, less OVERLAP_SECONDS for rows
stamped before it but committed after. resultTime is the arrival time, also
set again when a result is edited; QC's resultQuality PATCHes leave it, so
they don't come back. The first run of a datastream checks all of it. One
watermark per datastream is kept in STATE_FILE.
"""

import json
import os
import time

import pandas as pd
from istsos4_client import Client, Datastream, Observation
from requests.adapters import HTTPAdapter

from qc_historical import check_datastream


class Rebase(HTTPAdapter):
    """Send requests for public (the API's HOSTNAME + SUBPATH + VERSION, the
    start of its @iot.nextLink) to url, which this container can reach."""

    def __init__(self, public, url):
        super().__init__()
        self.public, self.url = public, url

    def send(self, request, **kwargs):
        request.url = self.url + request.url[len(self.public):]
        return super().send(request, **kwargs)


def first_time(client, filter):
    """phenomenonTime of the earliest observation of the query, None if
    none."""
    for o in client.iter_list(Observation, filter=filter,
                              orderby="phenomenonTime",
                              select="phenomenonTime", top=1):
        return pd.to_datetime(o.phenomenon_time, utc=True)
    return None


def update(client, datastream, mark, overlap, commit_message):
    """Check what is new in the datastream since mark (None: all of it).
    Return the next mark, an ISO timestamp."""
    # ponytail: the container's clock as watermark against the writers'
    # resultTime; the overlap covers skew under OVERLAP_SECONDS
    now = pd.Timestamp.now(tz="UTC").floor("s")
    start = None
    if mark is not None:
        # index (datastream_id, "resultTime"): see database/README.md for
        # databases created before it
        since = (pd.Timestamp(mark) - overlap).isoformat()
        start = first_time(client, f"Datastream/id eq {datastream.iot_id} "
                                   f"and resultTime gt {since}")
        if start is None:  # nothing new
            return now.isoformat()
    check_datastream(client, datastream, start, None,
                     commit_message=commit_message)
    return now.isoformat()


def save(path, state):
    """Replace the state file whole, so a crash never leaves half of it."""
    with open(f"{path}.tmp", "w") as f:
        json.dump(state, f, indent=1, sort_keys=True)
    os.replace(f"{path}.tmp", path)


def main():
    url = os.environ["ISTSOS4_URL"].rstrip("/")
    client = Client(
        url,
        os.getenv("ISTSOS4_USERNAME") or None,  # none with AUTHORIZATION=0
        os.getenv("ISTSOS4_PASSWORD") or None,
    )
    public = (os.getenv("ISTSOS4_PUBLIC_URL") or url).rstrip("/")
    if public != url:
        # ponytail: istsos4-client's own session; drop this once the client
        # rebases @iot.nextLink on its base_url
        client._session.mount(public, Rebase(public, url))
    commit_message = os.getenv("COMMIT_MESSAGE") or None
    interval = float(os.getenv("INTERVAL_MINUTES", "10")) * 60
    overlap = pd.Timedelta(float(os.getenv("OVERLAP_SECONDS", "60")), "s")
    if interval <= 0 or overlap < pd.Timedelta(0):
        raise ValueError("INTERVAL_MINUTES must be > 0, OVERLAP_SECONDS >= 0")
    names, excluded = [
        [p.strip() for p in os.getenv(key, "").split(",") if p.strip()]
        for key in ("DATASTREAMS", "EXCLUDE_DATASTREAMS")
    ]
    path = os.getenv("STATE_FILE", "/data/state.json")
    try:
        with open(path) as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {}
    print(f"qc: every {interval / 60:g} min, "
          f"{len(state)} datastreams in {path}")

    while True:
        started = time.monotonic()
        try:
            for datastream in client.list(Datastream):
                if (
                    not (datastream.properties or {}).get("constraints")
                    or datastream.name in excluded
                    or (names and datastream.name not in names)
                ):
                    continue
                key = str(datastream.iot_id)
                try:  # a datastream that fails keeps its mark: next run again
                    mark = update(client, datastream, state.get(key),
                                  overlap, commit_message)
                except Exception as error:
                    print(f"ERROR {datastream.name}: {error!r}")
                    continue
                if mark != state.get(key):
                    state[key] = mark
                    save(path, state)
        except Exception as error:  # e.g. API down: next run again
            print(f"ERROR {error!r}")
        time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
