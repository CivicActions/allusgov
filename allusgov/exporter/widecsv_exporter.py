"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import csv
import re

from bigtree import Node

from allusgov.cli_options import logger
from allusgov.models.exporter_base import FlatBaseExporter
from allusgov.models.registry import EXPORTERS


@EXPORTERS.register("widecsv")
class WideCSVExporter(FlatBaseExporter):
    """
    Export the flattened tree as a wide CSV file.

    This results in CSV files that only contain attribute values, but because
    of lists in the attribute data the number of columns can be very large.
    """

    format_key = "widecsv"

    def __init__(self, source: str, tree: Node) -> None:
        super().__init__(source, tree)

    def export(self, **kwargs) -> None:
        logger.info("Saving the %s tree in wide CSV format...", self.source)
        with open(self.export_path("csv", "wide"), "w", encoding="utf8") as f:
            orgs_flat, attrib_names = self.flatten(max_depth=2)
            skip_attribs = []
            # TODO: This approach is a hacky and slow, but it works for now.
            for attrib in attrib_names:
                # Skip attributes that include a list longer than 10 items.
                match = re.search(r"^(.*)\d{2,}", attrib)
                if match:
                    skip_attribs.append(match.group(1))
                # Also skip elements that include more than one list.
                match = re.search(r"^(.*)_\d+_.+_\d+_", attrib)
                if match:
                    skip_attribs.append(match.group(1))
            final_attrib_names = []
            for attrib_name in attrib_names:
                skip = False
                for skip_attrib in skip_attribs:
                    if attrib_name.startswith(skip_attrib):
                        skip = True
                if not skip:
                    final_attrib_names.append(attrib_name)
            writer = csv.DictWriter(
                f, fieldnames=final_attrib_names, lineterminator="\n"
            )
            writer.writeheader()
            for org in orgs_flat:
                final_attribs = {}
                del org["node"]
                for attrib_name, value in org.items():
                    if attrib_name in final_attrib_names:
                        final_attribs[attrib_name] = value
                writer.writerow(final_attribs)
