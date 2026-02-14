"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import Any

from bigtree import Node, tree_to_dataframe

from allusgov.cli_options import logger
from allusgov.models.exporter_base import ExporterBase
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("csv")
class CSVExporter(ExporterBase):
    """This results in more manageable CSV files, but the attributes end up as embedded JSON."""

    format_key = "csv"

    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        logger.info("Saving the %s graph in CSV format...", source)
        df = tree_to_dataframe(tree, all_attrs=True)
        df.to_csv(self.export_path(source=source, ext="csv"), index=False)
