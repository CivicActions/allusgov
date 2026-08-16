"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import Any

import networkx as nx
from bigtree import Node
from loguru import logger

from allusgov.exporter.exporter_base import NetworkXBaseExporter
from allusgov.registry.registry import EXPORTERS


@EXPORTERS.register("graphml")
class GraphMLExporter(NetworkXBaseExporter):
    format_key = "graphml"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        graph = self.build_graph(tree=tree)
        logger.info("Saving the {} graph in GraphML format...", source)
        nx.write_graphml(graph, self.export_path(source=source, ext="graphml"))
