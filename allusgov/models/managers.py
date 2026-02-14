"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/gov#license.
"""

from dataclasses import dataclass
from typing import Any

from bigtree import Node

from allusgov import load_plugins_once
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.importer_base import ImporterBase
from allusgov.models.registry import EXPORTERS, IMPORTERS


@dataclass
class ImportManager:

    importer_key: str

    def run(self, source: str) -> Node:
        load_plugins_once()
        importer_cls = IMPORTERS.get(self.importer_key)
        importer: ImporterBase = importer_cls()
        return importer.build(source=source)


@dataclass
class ExportManager:

    @staticmethod
    def export(fmt: str, source: str, root: Node, **kwargs: Any) -> None:
        load_plugins_once()
        exporter_cls = EXPORTERS.get(fmt)
        exporter: ExporterBase = exporter_cls()
        exporter.export(source=source, tree=root, **kwargs)

    @staticmethod
    def formats() -> list[str]:
        return EXPORTERS.keys()
