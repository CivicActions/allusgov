"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from bigtree import Node, tree_to_dot

from allusgov.cli_options import logger
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("dot")
class DotExporter(ExporterBase):
    format_key = "dot"

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs) -> None:
        logger.info("Saving the %s graph in DOT format...", self.source)
        export_path = self.export_path(ext="dot")
        tree_to_dot(self.tree).write(export_path.as_posix(), encoding="utf8")
