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

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs: Any) -> None:
        logger.info("Saving the %s graph in Cytoscape JSON format...", self.source)
        with open(self.export_path("cyjs"), "w", encoding="utf8") as f:
            json.dump(
                nx.cytoscape_data(self.G)["elements"], f, indent=2, sort_keys=True
            )
