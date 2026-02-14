"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import json
from typing import Any

from bigtree import Node, tree_to_dict, tree_to_nested_dict

from allusgov.cli_options import logger
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("json")
class JSONExporter(ExporterBase):
    format_key = "json"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the %s tree in JSON flat format...", source)
        with open(
            self.export_path(source=source, ext="json", suffix="flat"),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(tree_to_dict(tree, all_attrs=True), f, indent=2, sort_keys=True)

        logger.info("Saving the %s tree in JSON tree format...", source)
        with open(
            self.export_path(source=source, ext="json", suffix="tree"),
            "w",
            encoding="utf8",
        ) as f:
            json.dump(
                tree_to_nested_dict(tree, all_attrs=True),
                f,
                indent=2,
                sort_keys=True,
            )
