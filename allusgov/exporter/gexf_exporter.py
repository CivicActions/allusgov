"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import fileinput
from datetime import datetime

import networkx as nx
from bigtree import Node
from loguru import logger

from allusgov.models.exporter_base import NetworkXBaseExporter
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("gexf")
class GEXFExporter(NetworkXBaseExporter):
    format_key = "gexf"

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs) -> None:
        logger.info("Saving the %s graph in GEXF format...", self.source)
        nx.write_gexf(self.G, self.export_path("gexf"))
        # Update the file to remove the lastmodifieddate attribute, which generates spurious diffs
        date = datetime.now().strftime("%Y-%m-%d")
        with fileinput.input(self.export_path(ext="gexf"), inplace=True) as file:
            for line in file:
                line = line.replace(' lastmodifieddate="' + date + '"', "")
                print(line, end="")
