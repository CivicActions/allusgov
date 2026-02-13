"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/gov#license.
"""

from dataclasses import dataclass
from typing import Any

from bigtree import Node

from allusgov import load_plugins_once

# from allusgov.models.importer_base import ImporterBase
from allusgov.models.registry import EXPORTERS

# @dataclass
# class ImportManager:
#
#     importer_key: str
#
#     def run(self, source: str) -> Node:
#         load_plugins_once()
#         importer_cls = IMPORTERS.get(self.importer_key)
#         importer: ImporterBase = importer_cls(source=source)
#         return importer.import_path()


@dataclass
class ExportManager:

    @staticmethod
    def export(fmt: str, source: str, root: Node, **kwargs: Any) -> None:
        load_plugins_once()
        exporter_cls = EXPORTERS.get(fmt)
        exporter_cls(source=source, tree=root)

    @staticmethod
    def formats() -> list[str]:
        return EXPORTERS.keys()
