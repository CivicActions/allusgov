"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from io import TextIOWrapper
from typing import Any

from bigtree import Node, yield_tree
from loguru import logger

from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS
from allusgov.utils.utils import full_name


@EXPORTERS.register("text")
class TextTreeExporter(ExporterBase):
    format_key = "text"

    @staticmethod
    def print_tree(
        tree: Node, source: str = "samgov", file: TextIOWrapper | None = None
    ) -> None:
        for branch, stem, org in yield_tree(tree):
            attrs = {}
            for key, value in org.describe(
                exclude_prefix="_", exclude_attributes=["name"]
            ):
                attrs[key] = value
            sources = ""
            if source == "merged":
                sources = " - " + ", ".join(attrs.keys())
            name = full_name(org, source)
            print(f"{branch}{stem}{name}{sources}", file=file)

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the %s tree in text format...", source)
        with open(
            self.export_path(source=source, ext="txt"), "w", encoding="utf8"
        ) as f:
            self.print_tree(tree=tree, source=source, file=f)
