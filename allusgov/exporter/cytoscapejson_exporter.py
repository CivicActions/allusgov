"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import json
from typing import Any

import networkx as nx
from bigtree import Node
from loguru import logger

from allusgov.models.exporter_base import NetworkXBaseExporter
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("cyjs")
class CytoscapeJSONExporter(NetworkXBaseExporter):
    format_key = "cyjs"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the {} graph in Cytoscape JSON format...", source)
        graph = self.build_graph(tree=tree)
        with open(
            self.export_path(source=source, ext="cyjs"), "w", encoding="utf8"
        ) as f:
            json.dump(nx.cytoscape_data(graph)["elements"], f, indent=2, sort_keys=True)
