# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
STA to DCAT-AP 3.0 transformer.

Consumes a HarvestedCatalog or HarvestedNetworkCatalog from
app.v1.connector.harvester and builds one or more independent rdflib.Graph
objects -- never a single quad store. Each returned Graph is a complete,
self-contained DCAT-AP 3.0 record for one scope (root, orphan, or one
Network) and is serialized and cached whole by cache.py; there is no
cross-scope merge to invalidate.

Pivot rule (same as STAC, per STA-DCAT-AP-Transformation-Layer-Reference.md):
    Datastream  -> dcat:Dataset        (one per Datastream dict)
    Thing       -> dcat:DatasetSeries  (groups its Datastreams, scoped)
    STA root    -> dcat:Catalog + dcat:DataService (dual-typed, root scope only)
    Network     -> dcat:Catalog (sub-catalog, one graph per Network)

Scoping (NETWORK=1):
    root graph   -- Catalog + DataService + Agents + dcat:hasPart links only.
                    Carries no Dataset/DatasetSeries content itself, this is
                    the DCAT equivalent of the STAC root's child links.
    orphan graph -- its own dcat:Catalog, all Datastreams with no assigned
                    Network, grouped by Thing exactly like root does under
                    NETWORK=0.
    network graph(s) -- one dcat:Catalog per Network, scoped the same way.

A DatasetSeries (Thing) that has Datastreams split across scopes gets its
own independent node -- with the same URI -- rebuilt once per scope with
that scope's own extent. It never spans scopes in a single graph; a Dataset
(Datastream) belongs to exactly one scope, so its own extent is unambiguous
and never conflicts across graphs. Uniqueness within a scope's Turtle output
is enforced by which graph the triples live in, not by suffixing identifiers.

Mapping decisions: STA-DCAT-AP-Transformation-Layer-Reference.md

Public interface:
    build_dcat_catalog(catalog)                 -> dict[str, Graph]
    build_dcat_catalog_with_networks(net_catalog) -> dict[str, Graph | dict[int, Graph]]
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Union

import asyncpg
from rdflib import RDF, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, SKOS, XSD

from app import HOSTNAME, SUBPATH, VERSION
from app.v1.connector.harvester import HarvestedThing

logger = logging.getLogger(__name__)


# Namespaces

DCAT = Namespace("http://www.w3.org/ns/dcat#")
DCT = DCTERMS
LOCN = Namespace("http://www.w3.org/ns/locn#")
GEOSPARQL = Namespace("http://www.opengis.net/ont/geosparql#")
ADMS = Namespace("http://www.w3.org/ns/adms#")
ORG = Namespace("http://www.w3.org/ns/org#")

# DCAT-AP 3.0 profile conformance URI, asserted as dct:conformsTo on every
# Catalog node (root, orphan, and each Network sub-catalog).
_DCAT_AP_PROFILE_URI = URIRef("https://semiceu.github.io/DCAT-AP/releases/3.0.0/")

# Same OGC SensorThings conformance URI stac_transformer.py's DataService
# equivalent would use -- asserted on the root DataService node only.
_STA_CONFORMANCE_URI = URIRef("http://www.opengis.net/spec/iot_sensing/1.1/req/datamodel")

_MEDIA_JSON = "application/json"
_MEDIA_CSV = "text/csv"
_FILE_TYPE_JSON = URIRef("http://publications.europa.eu/resource/authority/file-type/JSON")
_FILE_TYPE_CSV = URIRef("http://publications.europa.eu/resource/authority/file-type/CSV")
_IANA_JSON = URIRef("https://www.iana.org/assignments/media-types/application/json")
_IANA_CSV = URIRef("https://www.iana.org/assignments/media-types/text/csv")

# Independent of STAC_ROOT_HREF in stac_transformer.py -- duplicated on
# purpose, see the "no cross-transformer imports" note in the module
# docstring's design rationale (kept out of code comments to avoid drift
# with the conversation it came from; the reasoning is: neither transformer
# should have to depend on the other existing).
DCAT_ROOT_HREF = f"{HOSTNAME}{SUBPATH}{VERSION}/connector/dcat"


def _bind_namespaces(g: Graph) -> None:
    g.bind("dcat", DCAT)
    g.bind("dct", DCT)
    g.bind("foaf", FOAF)
    g.bind("org", ORG)
    g.bind("locn", LOCN)
    g.bind("geosparql", GEOSPARQL)
    g.bind("skos", SKOS)
    g.bind("owl", OWL)
    g.bind("adms", ADMS)
    g.bind("xsd", XSD)


# Temporal helpers (duplicated from stac_transformer.py's logic on purpose --
# both transformers consume the exact same phenomenon_time shape from the
# harvester, but are kept independent so neither imports the other)


def _parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logger.warning("Could not parse ISO 8601 datetime: %r", value)
        return None


def _parse_phenomenon_time(
    phenomenon_time: Union["asyncpg.Range", str, None],
) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Parse a Datastream phenomenon_time value into (start, end) datetimes.
    Returns (None, None) when unusable -- see stac_transformer.py's
    _parse_phenomenon_time for the full rationale on asyncpg.Range handling.
    """
    if phenomenon_time is None:
        return None, None

    if isinstance(phenomenon_time, str):
        parts = phenomenon_time.split("/", 1)
        start_str = parts[0].strip()
        end_str = parts[1].strip() if len(parts) > 1 else ""
        start = _parse_iso(start_str)
        end = _parse_iso(end_str) if end_str and end_str != ".." else None
        return start, end

    if getattr(phenomenon_time, "isempty", False):
        return None, None

    start = phenomenon_time.lower
    end = phenomenon_time.upper
    if start is not None and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return start, end


# Spatial helpers (bbox math duplicated from stac_transformer.py, same
# independence rationale as the temporal helpers above)


def _extract_all_coordinates(geometry: dict) -> list[list[float]]:
    geom_type = geometry.get("type", "")
    coords = geometry.get("coordinates")

    if geom_type == "Point" and coords:
        return [coords[:2]]
    if geom_type in ("MultiPoint", "LineString") and coords:
        return [c[:2] for c in coords]
    if geom_type == "Polygon" and coords:
        return [c[:2] for c in coords[0]]
    if geom_type == "MultiPolygon" and coords:
        result: list[list[float]] = []
        for polygon in coords:
            result.extend(c[:2] for c in polygon[0])
        return result
    if geom_type == "GeometryCollection":
        result = []
        for geom in geometry.get("geometries", []):
            result.extend(_extract_all_coordinates(geom))
        return result
    return []


def _bbox_from_geometry(geometry: Optional[dict]) -> Optional[list[float]]:
    if geometry is None:
        return None
    coords = _extract_all_coordinates(geometry)
    if not coords:
        return None
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return [min(lons), min(lats), max(lons), max(lats)]


def _union_bboxes(bboxes: list[list[float]]) -> Optional[list[float]]:
    if not bboxes:
        return None
    return [
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    ]


def _bbox_to_polygon(bbox: list[float]) -> dict:
    """Turn a [minx, miny, maxx, maxy] bbox into a GeoJSON Polygon ring."""
    minx, miny, maxx, maxy = bbox
    return {
        "type": "Polygon",
        "coordinates": [[
            [minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny],
        ]],
    }


def _resolve_dataset_geometry(thing: HarvestedThing, ds: dict) -> Optional[dict]:
    """
    Fallback chain for a Dataset's dct:spatial geometry:
      1. Datastream.observed_area (preferred, per-variable footprint)
      2. First Thing.locations[0].geometry
      3. None (no dct:spatial triple emitted)
    """
    observed_area = ds.get("observed_area")
    if observed_area is not None:
        return observed_area
    if thing.locations:
        first_geom = thing.locations[0].get("geometry")
        if first_geom is not None:
            return first_geom
    return None


def _add_spatial(g: Graph, subject: Union[URIRef, BNode], geometry: Optional[dict]) -> None:
    """
    Add a dct:spatial dct:Location node carrying a GeoJSON geometry literal.
    No-op when geometry is None.
    """
    if not geometry:
        return
    loc_node = BNode()
    g.add((subject, DCT.spatial, loc_node))
    g.add((loc_node, RDF.type, DCT.Location))
    g.add((
        loc_node,
        LOCN.geometry,
        Literal(json.dumps(geometry), datatype=GEOSPARQL.geoJSONLiteral),
    ))


def _add_temporal(
    g: Graph, subject: Union[URIRef, BNode], start: Optional[datetime], end: Optional[datetime]
) -> None:
    """
    Add a dct:temporal PeriodOfTime blank node. dcat:endDate is omitted for
    open-ended intervals -- its absence correctly represents "still live".
    No-op when start is None.
    """
    if start is None:
        return
    period = BNode()
    g.add((subject, DCT.temporal, period))
    g.add((period, RDF.type, DCT.PeriodOfTime))
    g.add((period, DCAT.startDate, Literal(start.isoformat(), datatype=XSD.dateTime)))
    if end is not None:
        g.add((period, DCAT.endDate, Literal(end.isoformat(), datatype=XSD.dateTime)))
