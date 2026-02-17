"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import Any

from bigtree import Node, tree_to_dot
from loguru import logger

from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("dot")
class DotExporter(ExporterBase):
    format_key = "dot"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the {} graph in DOT format...", source)
        export_path = self.export_path(source=source, ext="dot")
        tree_to_dot(tree).write(export_path.as_posix(), encoding="utf8")
