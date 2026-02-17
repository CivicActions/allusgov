"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import fileinput
from datetime import datetime
from typing import Any

import networkx as nx
from bigtree import Node
from loguru import logger

from allusgov.models.exporter_base import NetworkXBaseExporter
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("gexf")
class GEXFExporter(NetworkXBaseExporter):
    format_key = "gexf"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the {} graph in GEXF format...", source)
        graph = self.build_graph(tree=tree)
        nx.write_gexf(graph, self.export_path(source=source, ext="gexf"))
        # Update the file to remove the lastmodifieddate attribute, which generates spurious diffs
        date = datetime.now().strftime("%Y-%m-%d")
        with fileinput.input(
            self.export_path(source=source, ext="gexf"), inplace=True
        ) as file:
            for line in file:
                line = line.replace(' lastmodifieddate="' + date + '"', "")
                print(line, end="")
