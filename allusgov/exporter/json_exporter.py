"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import json

from bigtree import Node, tree_to_dict, tree_to_nested_dict

from allusgov.cli_options import logger
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("json")
class JSONExporter(ExporterBase):
    format_key = "json"

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs) -> None:
        logger.info("Saving the %s tree in JSON flat format...", self.source)
        with open(
            self.export_path(ext="json", suffix="flat"), "w", encoding="utf8"
        ) as f:
            json.dump(
                tree_to_dict(self.tree, all_attrs=True), f, indent=2, sort_keys=True
            )

        logger.info("Saving the %s tree in JSON tree format...", self.source)
        with open(
            self.export_path(ext="json", suffix="tree"), "w", encoding="utf8"
        ) as f:
            json.dump(
                tree_to_nested_dict(self.tree, all_attrs=True),
                f,
                indent=2,
                sort_keys=True,
            )
