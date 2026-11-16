"""Checks update() against a fake server, and Rebase. Run it in the image,
from the repo:

docker run --rm --entrypoint python \
  -v "$PWD/utils/services/qc/test_qc.py:/app/test_qc.py:ro" \
  ghcr.io/istsos/istsos4/utils/qc:0.1 test_qc.py
"""

from types import SimpleNamespace

import pandas as pd
import requests

import qc

DATASTREAM = SimpleNamespace(iot_id=5, name="ds")
OVERLAP = pd.Timedelta(60, "s")
MARK = "2026-01-01T12:00:00+00:00"


class Server:
    """Answers with these phenomenonTimes, as if they all match the filter."""

    def __init__(self, *times):
        self.times, self.filters = times, []

    def iter_list(self, entity, filter, orderby, **_):
        self.filters.append(filter)
        ordered = sorted(self.times, reverse=orderby.endswith("desc"))
        return iter([SimpleNamespace(phenomenon_time=t) for t in ordered])


def update(server, mark):
    """(next mark, check_datastream starts, filters sent)"""
    starts = []
    qc.check_datastream = lambda client, ds, start, end, **_: starts.append(
        start
    )
    next_mark = qc.update(server, DATASTREAM, mark, OVERLAP, None)
    return next_mark, starts, server.filters


# First run: all of it, no query, mark = now.
mark, starts, filters = update(Server(), None)
assert starts == [None] and filters == [], (starts, filters)
assert pd.Timestamp(mark) > pd.Timestamp(MARK)

# From the oldest phenomenonTime with a resultTime after mark - overlap.
mark, starts, filters = update(
    Server("2026-01-01T10:20:00Z", "2026-01-01T09:00:00Z"), MARK
)
assert starts == [pd.Timestamp("2026-01-01T09:00:00", tz="UTC")], starts
assert filters == [
    "Datastream/id eq 5 and resultTime gt 2026-01-01T11:59:00+00:00"
], filters
assert pd.Timestamp(mark) > pd.Timestamp(MARK)

# Nothing new: no check, mark moves on.
mark, starts, _ = update(Server(), MARK)
assert starts == [] and pd.Timestamp(mark) > pd.Timestamp(MARK)

# A link the API built from HOSTNAME goes to ISTSOS4_URL instead.
sent = []
qc.HTTPAdapter.send = lambda self, request, **_: sent.append(request.url)
link = "http://localhost:8018/istsos4/v1.1/Datastreams?$top=2&$skip=2"
qc.Rebase(
    "http://localhost:8018/istsos4/v1.1", "http://api:5000/istsos4/v1.1"
).send(requests.Request("GET", link).prepare())
assert sent == ["http://api:5000/istsos4/v1.1/Datastreams?$top=2&$skip=2"], sent

print("ok")
