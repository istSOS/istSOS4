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
Linear-time Turtle serialization for the DCAT-AP graphs.

rdflib's TurtleSerializer asks the NamespaceManager for a prefixed name for
every IRI in the graph, subjects and objects included. compute_qname inserts
the namespace part of each IRI into a trie and searches one trie level with a
linear startswith scan (namespace.get_longest_namespace). The connector's
per-entity IRIs (.../datasets/datastream-{id}, .../Datastreams({id})/...) all
land on the same trie level, so each lookup is O(n) and the document is O(n^2):
about 25 s for 124k triples and 480 s for one FROST cycle. rdflib 7.1.1 and
7.6.0 behave the same.

The serializer below only asks rdflib for a prefixed name when the IRI falls
under a namespace already bound on the graph (the dozen DCAT-AP vocabularies
bound in dcat_transformer._bind_namespaces). Every other IRI is written in full
as <...>, which is what rdflib already did for unbound subjects and objects, so
the parsed graph is identical; only unbound predicates lose an auto-generated
nsN: prefix. The trie then only ever holds vocabulary namespaces, and the cost
is linear in the number of triples.
"""

from __future__ import annotations

import io

from rdflib import Graph, URIRef
from rdflib.plugins.serializers.turtle import TurtleSerializer


class _BoundPrefixTurtleSerializer(TurtleSerializer):
    def __init__(self, store: Graph) -> None:
        super().__init__(store)
        self._bound = tuple(sorted({str(ns) for _, ns in store.namespaces()}, key=len, reverse=True))

    def getQName(self, uri, gen_prefix=True):  # noqa: N802 -- rdflib's name
        if not isinstance(uri, URIRef) or not str(uri).startswith(self._bound):
            return None
        return super().getQName(uri, gen_prefix=False)


def serialize_turtle(graph: Graph) -> bytes:
    """Serialize ``graph`` as UTF-8 Turtle in linear time (see module docstring)."""
    buf = io.BytesIO()
    _BoundPrefixTurtleSerializer(graph).serialize(buf, encoding="utf-8")
    return buf.getvalue()
