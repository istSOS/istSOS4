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
from app.v1.connector.config import Settings, get_settings
from app.v1.connector.harvester import HarvestedCatalog, HarvestedNetworkCatalog, HarvestedThing

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


# Keyword / description helpers

def _extract_keywords(ds: dict, thing: HarvestedThing) -> list[str]:
    """
    Deduplicated keyword list for a Dataset. Sources, in order:
      - ObservedProperty.name, split on ":" (dummy data uses
        "category:subcategory:phenomenon_id" notation)
      - Thing.name
      - Datastream.properties["keywords"]
    """
    seen: set[str] = set()
    keywords: list[str] = []

    def _add(kw: str) -> None:
        kw = kw.strip()
        if kw and kw not in seen:
            seen.add(kw)
            keywords.append(kw)

    op = ds.get("observed_property")
    if op and op.get("name"):
        for part in op["name"].split(":"):
            _add(part)
    _add(thing.name)
    for kw in (ds.get("properties") or {}).get("keywords", []):
        if isinstance(kw, str):
            _add(kw)
    return keywords


def _extract_series_keywords(thing: HarvestedThing, datastreams: list[dict]) -> list[str]:
    """Deduplicated keyword list for a DatasetSeries: Thing.name plus every
    keyword its scope's own Datastreams would individually carry."""
    seen: set[str] = set()
    keywords: list[str] = []

    def _add(kw: str) -> None:
        kw = kw.strip()
        if kw and kw not in seen:
            seen.add(kw)
            keywords.append(kw)

    _add(thing.name)
    for ds in datastreams:
        for kw in _extract_keywords(ds, thing):
            _add(kw)
    return keywords


def _compose_dataset_description(ds: dict, thing: HarvestedThing) -> str:
    parts: list[str] = []
    if ds.get("description"):
        parts.append(ds["description"])
    op = ds.get("observed_property")
    if op and op.get("description"):
        parts.append(op["description"])
    sensor = ds.get("sensor")
    if sensor and sensor.get("description"):
        parts.append(sensor["description"])
    return " | ".join(p for p in parts if p) or ds.get("name", "")


# STA href reconstruction (duplicated from stac_transformer.py -- the
# harvested dicts carry no self_link field by design; see harvester.py's
# contract. Both transformers derive the same URL from the same HOSTNAME /
# SUBPATH / VERSION constants, so there is no risk of the two disagreeing
# even without importing one from the other.)

def _datastream_href(ds_id) -> str:
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Datastreams({ds_id})"


def _thing_href(thing_id) -> str:
    return f"{HOSTNAME}{SUBPATH}{VERSION}/Things({thing_id})"


# DCAT resource URI builders

def _dataset_uri(ds_id) -> URIRef:
    return URIRef(f"{DCAT_ROOT_HREF}/datasets/datastream-{ds_id}")


def _series_uri(thing_id) -> URIRef:
    """
    Same URI regardless of scope -- a DatasetSeries rebuilt once per scope
    intentionally reuses this identifier. Uniqueness is enforced by which
    Graph the triples live in, not by suffixing the identifier.
    """
    return URIRef(f"{DCAT_ROOT_HREF}/series/thing-{thing_id}")


def _distribution_uri(ds_id, kind: str) -> URIRef:
    return URIRef(f"{DCAT_ROOT_HREF}/datasets/datastream-{ds_id}/distributions/{kind}")


def _catalog_uri(network_id: Optional[int] = None, orphan: bool = False) -> URIRef:
    if network_id is not None:
        return URIRef(f"{DCAT_ROOT_HREF}/{network_id}")
    if orphan:
        return URIRef(f"{DCAT_ROOT_HREF}/orphan")
    return URIRef(DCAT_ROOT_HREF)


# Publisher agent

def _add_publisher_agent(g: Graph, settings: Settings) -> Optional[URIRef]:
    """
    Add the foaf:Agent / org:Organization node for the publisher. Returns
    the URIRef when DCAT_PUBLISHER_URI is set, None when only
    DCAT_PUBLISHER_NAME is set (BNode emitted, but not returned -- a BNode
    publisher can't be referenced across the independent per-scope graphs
    this transformer produces, since each is cached and served separately)
    or when DCAT_PUBLISHER_NAME itself is unset (no publisher at all).
    """
    if not settings.DCAT_PUBLISHER_NAME:
        return None

    if settings.DCAT_PUBLISHER_URI:
        pub_node: Union[URIRef, BNode] = URIRef(settings.DCAT_PUBLISHER_URI)
    else:
        pub_node = BNode()
        logger.warning(
            "DCAT_PUBLISHER_URI is not set -- publisher will be a blank node "
            "and cannot be referenced from outside this graph."
        )

    g.add((pub_node, RDF.type, FOAF.Agent))
    g.add((pub_node, RDF.type, ORG.Organization))
    g.add((pub_node, FOAF.name, Literal(settings.DCAT_PUBLISHER_NAME)))
    if settings.DCAT_PUBLISHER_HOMEPAGE:
        g.add((pub_node, FOAF.homepage, URIRef(settings.DCAT_PUBLISHER_HOMEPAGE)))
    if settings.DCAT_PUBLISHER_MBOX:
        mbox = settings.DCAT_PUBLISHER_MBOX
        if not mbox.startswith("mailto:"):
            mbox = f"mailto:{mbox}"
        g.add((pub_node, FOAF.mbox, URIRef(mbox)))

    return pub_node if isinstance(pub_node, URIRef) else None


# Distribution builder

def _add_distributions(
    g: Graph, dataset_uri: URIRef, ds: dict, license_uri: Optional[str]
) -> None:
    """
    Add the three dcat:Distribution nodes for one Datastream dict, mirroring
    stac_transformer.py's three Item Assets one-for-one:
      1. observations_json -- GET Observations, JSON
      2. observations_csv  -- GET Observations, CSV bulk export
      3. datastream        -- the STA Datastream entity itself, as metadata
    """
    ds_id = ds.get("id")
    ds_name = ds.get("name", "")
    base_href = _datastream_href(ds_id)

    dist_json = _distribution_uri(ds_id, "observations-json")
    g.add((dataset_uri, DCAT.distribution, dist_json))
    g.add((dist_json, RDF.type, DCAT.Distribution))
    g.add((dist_json, DCT.title, Literal(f"{ds_name} \u2014 JSON observations feed")))
    g.add((dist_json, DCT.description, Literal(
        f"Live OGC SensorThings Observations feed for Datastream: {ds_name}"
    )))
    g.add((dist_json, DCAT.accessURL, URIRef(f"{base_href}/Observations")))
    g.add((dist_json, DCT.format, _FILE_TYPE_JSON))
    g.add((dist_json, DCAT.mediaType, _IANA_JSON))
    if license_uri:
        g.add((dist_json, DCT.license, URIRef(license_uri)))

    dist_csv = _distribution_uri(ds_id, "observations-csv")
    g.add((dataset_uri, DCAT.distribution, dist_csv))
    g.add((dist_csv, RDF.type, DCAT.Distribution))
    g.add((dist_csv, DCT.title, Literal(f"{ds_name} \u2014 CSV export")))
    g.add((dist_csv, DCT.description, Literal(
        f"CSV bulk export of Observations for Datastream: {ds_name}"
    )))
    csv_href = URIRef(f"{base_href}/Observations?$resultFormat=CSV")
    g.add((dist_csv, DCAT.accessURL, csv_href))
    g.add((dist_csv, DCAT.downloadURL, csv_href))
    g.add((dist_csv, DCT.format, _FILE_TYPE_CSV))
    g.add((dist_csv, DCAT.mediaType, _IANA_CSV))
    if license_uri:
        g.add((dist_csv, DCT.license, URIRef(license_uri)))

    dist_meta = _distribution_uri(ds_id, "datastream")
    g.add((dataset_uri, DCAT.distribution, dist_meta))
    g.add((dist_meta, RDF.type, DCAT.Distribution))
    g.add((dist_meta, DCT.title, Literal(f"STA Datastream entity: {ds_name}")))
    g.add((dist_meta, DCT.description, Literal(
        f"OGC SensorThings Datastream entity metadata for: {ds_name}"
    )))
    g.add((dist_meta, DCAT.accessURL, URIRef(base_href)))
    g.add((dist_meta, DCT.format, _FILE_TYPE_JSON))
    g.add((dist_meta, DCAT.mediaType, _IANA_JSON))
    if license_uri:
        g.add((dist_meta, DCT.license, URIRef(license_uri)))


# Dataset builder (Datastream dict -> dcat:Dataset)

def _build_dataset(
    g: Graph,
    thing: HarvestedThing,
    ds: dict,
    series_uri: URIRef,
    publisher_node: Optional[URIRef],
    settings: Settings,
) -> Optional[tuple[URIRef, Optional[list[float]], Optional[datetime], Optional[datetime]]]:
    """
    Add a dcat:Dataset node for one Datastream dict.

    Returns None (with a WARNING logged) when there is no parseable
    phenomenon_time -- same skip rule as a STAC Item without a datetime,
    a Dataset with no dct:temporal is allowed by the spec but is useless
    for a sensor-observation catalog, so it is dropped rather than emitted
    as a dead-end node.

    On success, returns (dataset_uri, bbox, start, end) so the caller can
    fold this Dataset's spatial/temporal footprint into its DatasetSeries'
    own scoped extent without a second pass over the Datastream dict.
    """
    ds_id = ds.get("id")
    start, end = _parse_phenomenon_time(ds.get("phenomenon_time"))
    if start is None:
        logger.warning(
            "Skipping Datastream %s in Thing %s: no usable phenomenon_time -- "
            "a Dataset without dct:temporal provides no value in this catalog",
            ds_id, thing.id,
        )
        return None

    dataset_uri = _dataset_uri(ds_id)
    lang = settings.DCAT_LANGUAGE

    g.add((dataset_uri, RDF.type, DCAT.Dataset))
    g.add((dataset_uri, DCT.identifier, Literal(f"datastream-{ds_id}")))

    title = f"{thing.name} \u2014 {ds.get('name', '')}"
    g.add((dataset_uri, DCT.title, Literal(title, lang=lang)))
    g.add((dataset_uri, DCT.description, Literal(_compose_dataset_description(ds, thing), lang=lang)))

    for kw in _extract_keywords(ds, thing):
        g.add((dataset_uri, DCAT.keyword, Literal(kw, lang=lang)))

    op = ds.get("observed_property")
    if op:
        op_def = op.get("definition")
        if op_def and str(op_def).startswith("http"):
            g.add((dataset_uri, DCAT.theme, URIRef(op_def)))
            g.add((dataset_uri, DCT.subject, URIRef(op_def)))
        # NOTE(future): observed_property_definition is a placeholder string
        # in dummy data, not a real OGC-style URI -- once that's addressed
        # upstream, dcat:theme/dct:subject will start resolving for every
        # Dataset instead of only the ones with a live URL already set.

    bbox = None
    geometry = _resolve_dataset_geometry(thing, ds)
    if geometry is not None:
        bbox = _bbox_from_geometry(geometry)
        _add_spatial(g, dataset_uri, geometry)

    _add_temporal(g, dataset_uri, start, end)

    obs_type = ds.get("observation_type")
    if obs_type:
        g.add((dataset_uri, DCT.conformsTo, URIRef(obs_type)))

    g.add((dataset_uri, DCAT.inSeries, series_uri))
    g.add((dataset_uri, DCT.isPartOf, series_uri))
    g.add((dataset_uri, OWL.sameAs, URIRef(_datastream_href(ds_id))))
    g.add((dataset_uri, DCAT.landingPage, URIRef(_datastream_href(ds_id))))

    if publisher_node:
        g.add((dataset_uri, DCT.publisher, publisher_node))

    props = ds.get("properties") or {}
    license_val = props.get("license") or settings.DCAT_DEFAULT_LICENSE
    if license_val:
        g.add((dataset_uri, DCT.license, URIRef(license_val)))

    access_rights = props.get("accessRights") or settings.DCAT_DEFAULT_ACCESS_RIGHTS
    if access_rights:
        g.add((dataset_uri, DCT.accessRights, URIRef(access_rights)))

    contact = props.get("contactPoint")
    if isinstance(contact, dict):
        from rdflib.namespace import Namespace as _NS
        vcard = _NS("http://www.w3.org/2006/vcard/ns#")
        vcard_node = BNode()
        g.add((dataset_uri, DCAT.contactPoint, vcard_node))
        g.add((vcard_node, RDF.type, vcard.Kind))
        if fn := contact.get("fn"):
            g.add((vcard_node, vcard.fn, Literal(fn)))
        if email := contact.get("email"):
            mailto = email if email.startswith("mailto:") else f"mailto:{email}"
            g.add((vcard_node, vcard.hasEmail, URIRef(mailto)))

    _add_distributions(g, dataset_uri, ds, license_val)

    return dataset_uri, bbox, start, end


# DatasetSeries builder (HarvestedThing -> dcat:DatasetSeries, scoped)

def _build_dataset_series(
    g: Graph,
    thing: HarvestedThing,
    scoped_datastreams: list[dict],
    dataset_records: list[tuple[URIRef, Optional[list[float]], Optional[datetime], Optional[datetime]]],
    catalog_uri: URIRef,
    publisher_node: Optional[URIRef],
    settings: Settings,
) -> URIRef:
    """
    Add a dcat:DatasetSeries node for a Thing, scoped to whatever subset of
    its Datastreams belong to this graph's scope.

    Spatial/temporal extent is computed only from this scope's own
    successfully-built Datasets (dataset_records), falling back to the
    Thing's own Location geometries when no Dataset contributed a bbox --
    exactly the STAC Collection extent's fallback chain, kept independent
    per scope rather than shared across the whole Thing.
    """
    series_uri = _series_uri(thing.id)
    lang = settings.DCAT_LANGUAGE

    g.add((series_uri, RDF.type, DCAT.DatasetSeries))
    g.add((series_uri, DCT.identifier, Literal(f"thing-{thing.id}")))
    g.add((series_uri, DCT.title, Literal(thing.name, lang=lang)))
    if thing.description:
        g.add((series_uri, DCT.description, Literal(thing.description, lang=lang)))

    for kw in _extract_series_keywords(thing, scoped_datastreams):
        g.add((series_uri, DCAT.keyword, Literal(kw, lang=lang)))

    for dataset_uri, _bbox, _start, _end in dataset_records:
        g.add((series_uri, DCAT.seriesMember, dataset_uri))

    bboxes = [r[1] for r in dataset_records if r[1] is not None]
    if bboxes:
        _add_spatial(g, series_uri, _bbox_to_polygon(_union_bboxes(bboxes)))
    elif thing.locations:
        first_geom = thing.locations[0].get("geometry")
        if first_geom is not None:
            _add_spatial(g, series_uri, first_geom)

    starts = [r[2] for r in dataset_records if r[2] is not None]
    ends = [r[3] for r in dataset_records if r[3] is not None]
    open_ended = any(r[3] is None for r in dataset_records if r[2] is not None)
    if starts:
        series_end = None if open_ended else (max(ends) if ends else None)
        _add_temporal(g, series_uri, min(starts), series_end)

    g.add((series_uri, DCT.isPartOf, catalog_uri))
    g.add((series_uri, OWL.sameAs, URIRef(_thing_href(thing.id))))
    g.add((series_uri, FOAF.homepage, URIRef(_thing_href(thing.id))))

    if publisher_node:
        g.add((series_uri, DCT.publisher, publisher_node))

    return series_uri


# Scope builder: Things -> (DatasetSeries + Dataset) triples in one Graph

def _build_scope(
    g: Graph,
    things: list[HarvestedThing],
    catalog_uri: URIRef,
    publisher_node: Optional[URIRef],
    settings: Settings,
) -> tuple[int, int, int]:
    """
    Populate g with one dcat:DatasetSeries per Thing and one dcat:Dataset
    per Datastream, all scoped to this single Graph. Returns
    (series_count, dataset_count, skipped_count).
    """
    dataset_count = 0
    skipped_count = 0

    for thing in things:
        records = []
        for ds in thing.datastreams:
            result = _build_dataset(g, thing, ds, _series_uri(thing.id), publisher_node, settings)
            if result is None:
                skipped_count += 1
                continue
            records.append(result)
            dataset_count += 1

        if not records and thing.datastreams:
            logger.warning(
                "Thing %s (%s): all %d Datastreams were skipped -- "
                "DatasetSeries will have no seriesMember",
                thing.id, thing.name, len(thing.datastreams),
            )

        series_uri = _build_dataset_series(
            g, thing, thing.datastreams, records, catalog_uri, publisher_node, settings
        )
        g.add((catalog_uri, DCAT.dataset, series_uri))
        for dataset_uri, _bbox, _start, _end in records:
            g.add((catalog_uri, DCAT.dataset, dataset_uri))

    return len(things), dataset_count, skipped_count


# Catalog / DataService builder (root scope only)

def _add_root_catalog_and_service(
    g: Graph,
    settings: Settings,
    publisher_node: Optional[URIRef],
    description: str,
    catalog_uri: URIRef,
    part_uris: list[URIRef],
) -> URIRef:
    """
    Add the dual-typed dcat:Catalog + dcat:DataService node for the STA
    service root. Used by both build_dcat_catalog (single scope) and
    build_dcat_catalog_with_networks (root graph, hasPart-only).
    """
    lang = settings.DCAT_LANGUAGE

    g.add((catalog_uri, RDF.type, DCAT.Catalog))
    g.add((catalog_uri, RDF.type, DCAT.DataService))
    g.add((catalog_uri, DCT.identifier, Literal(settings.DCAT_CATALOG_ID)))
    g.add((catalog_uri, DCT.conformsTo, _DCAT_AP_PROFILE_URI))
    g.add((catalog_uri, DCT.conformsTo, _STA_CONFORMANCE_URI))
    g.add((catalog_uri, DCAT.endpointURL, catalog_uri))
    g.add((catalog_uri, DCAT.endpointDescription, URIRef(f"{HOSTNAME}{SUBPATH}{VERSION}/Conformance")))

    if settings.DCAT_CATALOG_TITLE:
        g.add((catalog_uri, DCT.title, Literal(settings.DCAT_CATALOG_TITLE, lang=lang)))
    if settings.DCAT_CATALOG_DESCRIPTION:
        g.add((catalog_uri, DCT.description, Literal(
            f"{settings.DCAT_CATALOG_DESCRIPTION}. {description}", lang=lang
        )))
    g.add((catalog_uri, DCT.language, Literal(lang)))

    if settings.DCAT_DEFAULT_LICENSE:
        g.add((catalog_uri, DCT.license, URIRef(settings.DCAT_DEFAULT_LICENSE)))
    if settings.DCAT_DEFAULT_ACCESS_RIGHTS:
        g.add((catalog_uri, DCT.accessRights, URIRef(settings.DCAT_DEFAULT_ACCESS_RIGHTS)))

    if publisher_node:
        g.add((catalog_uri, DCT.publisher, publisher_node))

    for part_uri in part_uris:
        g.add((catalog_uri, DCT.hasPart, part_uri))

    return catalog_uri


def _add_sub_catalog(
    g: Graph,
    settings: Settings,
    publisher_node: Optional[URIRef],
    catalog_uri: URIRef,
    identifier: str,
    title: Optional[str],
    description: str,
) -> URIRef:
    """
    Add a plain dcat:Catalog node (no DataService typing) for a Network
    sub-catalog or the orphan scope. Mirrors stac_transformer.py's Network
    subcatalog dict one-for-one.
    """
    lang = settings.DCAT_LANGUAGE

    g.add((catalog_uri, RDF.type, DCAT.Catalog))
    g.add((catalog_uri, DCT.identifier, Literal(identifier)))
    g.add((catalog_uri, DCT.conformsTo, _DCAT_AP_PROFILE_URI))
    if title:
        g.add((catalog_uri, DCT.title, Literal(title, lang=lang)))
    g.add((catalog_uri, DCT.description, Literal(description, lang=lang)))
    g.add((catalog_uri, DCT.language, Literal(lang)))
    g.add((catalog_uri, DCT.isPartOf, _catalog_uri()))

    if settings.DCAT_DEFAULT_LICENSE:
        g.add((catalog_uri, DCT.license, URIRef(settings.DCAT_DEFAULT_LICENSE)))
    if publisher_node:
        g.add((catalog_uri, DCT.publisher, publisher_node))

    return catalog_uri


# Public interface

def build_dcat_catalog(catalog: HarvestedCatalog) -> dict[str, Graph]:
    """
    Build the NETWORK=0 DCAT-AP 3.0 graph: a single scope, everything in
    one Graph, root Catalog + DataService serving every DatasetSeries and
    Dataset directly. Mirrors build_stac_catalog()'s single-scope shape.

    Returns {"root": Graph}.
    """
    settings = get_settings()
    g = Graph()
    _bind_namespaces(g)

    publisher_node = _add_publisher_agent(g, settings)
    catalog_uri = _catalog_uri()

    if not settings.has_mandatory_dcat_fields:
        logger.warning(
            "DCAT_CATALOG_TITLE, DCAT_CATALOG_DESCRIPTION, or DCAT_PUBLISHER_NAME "
            "is not set -- the generated dcat:Catalog will be missing mandatory "
            "DCAT-AP 3.0 fields. Set these via environment variables or .env."
        )

    _add_root_catalog_and_service(
        g, settings, publisher_node,
        description=(
            f"{settings.DCAT_DEPLOYMENT_NAME} deployment: {catalog.thing_count} "
            f"Things, harvested at {catalog.harvested_at}."
        ),
        catalog_uri=catalog_uri,
        part_uris=[],
    )

    series_count, dataset_count, skipped = _build_scope(
        g, catalog.things, catalog_uri, publisher_node, settings
    )
    g.add((catalog_uri, DCAT.service, catalog_uri))

    logger.info(
        "DCAT transform complete: %d DatasetSeries, %d Datasets, %d skipped",
        series_count, dataset_count, skipped,
    )
    return {"root": g}


def build_dcat_catalog_with_networks(
    network_catalog: HarvestedNetworkCatalog,
) -> dict[str, Union[Graph, dict[int, Graph]]]:
    """
    Build the NETWORK=1 DCAT-AP 3.0 graphs:

        Graph (root)   -- Catalog + DataService + Agents + hasPart links only
        Graph (orphan) -- its own Catalog, orphan Things/Datastreams
        Graph (per Network) -- its own Catalog, that Network's Things/Datastreams

    Returns {"root": Graph, "orphan": Graph, "networks": {network_id: Graph, ...}}.

    Deliberately does NOT collapse orphan into root the way
    build_stac_catalog_with_networks() serves its orphan scope directly from
    the root Catalog -- see the module docstring's scoping section for why
    DCAT keeps root structurally empty of Dataset/DatasetSeries content.
    """
    settings = get_settings()

    if not settings.has_mandatory_dcat_fields:
        logger.warning(
            "DCAT_CATALOG_TITLE, DCAT_CATALOG_DESCRIPTION, or DCAT_PUBLISHER_NAME "
            "is not set -- the generated dcat:Catalog will be missing mandatory "
            "DCAT-AP 3.0 fields. Set these via environment variables or .env."
        )

    total_series = 0
    total_datasets = 0
    total_skipped = 0

    # --- orphan graph ---
    orphan_g = Graph()
    _bind_namespaces(orphan_g)
    orphan_pub = _add_publisher_agent(orphan_g, settings)
    orphan_catalog_uri = _catalog_uri(orphan=True)
    _add_sub_catalog(
        orphan_g, settings, orphan_pub, orphan_catalog_uri,
        identifier=f"{settings.DCAT_CATALOG_ID}-orphan",
        title="Unassigned Datastreams",
        description=(
            f"Datastreams with no assigned Network, harvested at "
            f"{network_catalog.harvested_at}."
        ),
    )
    o_series, o_datasets, o_skipped = _build_scope(
        orphan_g, network_catalog.orphan_things, orphan_catalog_uri, orphan_pub, settings
    )
    total_series += o_series
    total_datasets += o_datasets
    total_skipped += o_skipped

    # --- per-Network graphs ---
    network_graphs: dict[int, Graph] = {}
    for net in network_catalog.networks:
        net_g = Graph()
        _bind_namespaces(net_g)
        net_pub = _add_publisher_agent(net_g, settings)
        net_catalog_uri = _catalog_uri(network_id=net.id)
        things = network_catalog.things_by_network.get(net.id, [])
        _add_sub_catalog(
            net_g, settings, net_pub, net_catalog_uri,
            identifier=f"network-{net.id}",
            title=net.name or None,
            description=(
                f"{settings.DCAT_DEPLOYMENT_NAME} Network subcatalog: "
                f"{net.name} ({len(things)} Things)."
            ),
        )
        n_series, n_datasets, n_skipped = _build_scope(
            net_g, things, net_catalog_uri, net_pub, settings
        )
        total_series += n_series
        total_datasets += n_datasets
        total_skipped += n_skipped
        network_graphs[net.id] = net_g

    # --- root graph: structural only, no Dataset/DatasetSeries content ---
    root_g = Graph()
    _bind_namespaces(root_g)
    root_pub = _add_publisher_agent(root_g, settings)
    root_catalog_uri = _catalog_uri()
    part_uris = [orphan_catalog_uri] + [_catalog_uri(network_id=net.id) for net in network_catalog.networks]

    _add_root_catalog_and_service(
        root_g, settings, root_pub,
        description=(
            f"{settings.DCAT_DEPLOYMENT_NAME} deployment: {len(network_catalog.networks)} "
            f"Networks, harvested at {network_catalog.harvested_at}. Datastreams with no "
            "assigned Network are served from the orphan sub-catalog."
        ),
        catalog_uri=root_catalog_uri,
        part_uris=part_uris,
    )
    root_g.add((root_catalog_uri, DCAT.service, root_catalog_uri))
    root_g.add((root_catalog_uri, DCAT.catalog, orphan_catalog_uri))
    for net in network_catalog.networks:
        root_g.add((root_catalog_uri, DCAT.catalog, _catalog_uri(network_id=net.id)))

    logger.info(
        "DCAT network transform complete: %d Networks, %d DatasetSeries, "
        "%d Datasets, %d skipped",
        len(network_catalog.networks), total_series, total_datasets, total_skipped,
    )

    return {"root": root_g, "orphan": orphan_g, "networks": network_graphs}
