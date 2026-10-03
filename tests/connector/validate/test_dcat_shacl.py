"""
test_dcat_shacl.py -- validates the connector's DCAT-AP output against the
official DCAT-AP 3.0.0 SHACL shapes (SEMICeu).

Requires a reachable connector instance -- see conftest.py. Run alone with:
    pytest -v tests/connector/validate/test_dcat_shacl.py
"""

from __future__ import annotations

import sys

import pytest
from rdflib import Graph

# pyshacl's rdfs-inference pass over deeply nested DCAT-AP collection graphs
# (rdf:List-style dcat:distribution chains, etc.) can exceed Python's default
# recursion depth. 10000 is a deliberate, bounded bump -- not the 50000 the
# old script used, which risks a C-stack overflow/segfault rather than a
# catchable RecursionError. If this still isn't enough, the graph shape is
# the thing to look at, not this number.
sys.setrecursionlimit(10000)

pyshacl = pytest.importorskip("pyshacl")


@pytest.fixture(scope="module")
def data_graph(dcat_turtle_files) -> Graph:
    graph = Graph()
    for path in dcat_turtle_files:
        graph.parse(path, format="turtle")
    return graph


@pytest.fixture(scope="module")
def shapes_graph(shacl_shapes_file) -> Graph:
    graph = Graph()
    graph.parse(shacl_shapes_file, format="turtle")
    return graph


def test_dcat_output_has_triples(data_graph):
    # Guards against a false-positive "conforms" from an empty/near-empty graph.
    assert len(data_graph) > 0, "DCAT output graph is empty -- check the connector endpoints in conftest.py"


def test_dcat_output_conforms_to_dcat_ap_3(data_graph, shapes_graph):
    conforms, _results_graph, results_text = pyshacl.validate(
        data_graph,
        shacl_graph=shapes_graph,
        inference="rdfs",
    )
    assert conforms, f"DCAT-AP 3.0.0 SHACL violations:\n{results_text}"