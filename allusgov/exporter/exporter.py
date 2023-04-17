import csv
import datetime
import fileinput
import json
import os
import re
from io import TextIOWrapper
from logging import Logger
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import networkx as nx
from bigtree import (
    Node,
    levelorder_iter,
    tree_to_dataframe,
    tree_to_dict,
    tree_to_dot,
    tree_to_nested_dict,
    yield_tree,
)
from flatten_json import flatten
from natsort import natsorted
from networkx.classes.digraph import DiGraph

from ..utils.utils import full_name


class BaseExporter:
    def __init__(self, logger: Logger, source: str, tree: Node, data_dir: str) -> None:
        self.logger = logger
        self.source = source
        self.tree = tree
        self.data_dir = data_dir
        os.makedirs(data_dir + "/" + source, exist_ok=True)

    def export_path(self, ext: str, suffix: str = "") -> str:
        if suffix != "":
            suffix = "-" + suffix
        return (
            self.data_dir + "/" + self.source + "/" + self.source + suffix + "." + ext
        )

    def export(self):
        raise NotImplementedError()


class TextExporter(BaseExporter):
    def print_tree(
        self, tree: Node, source: str = "samgov", file: Optional[TextIOWrapper] = None
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

    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " tree in text format...")
        with open(self.export_path("txt"), "w", encoding="utf8") as f:
            self.print_tree(self.tree, self.source, f)


class JSONExporter(BaseExporter):
    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " tree in JSON flat format...")
        with open(self.export_path("json", "flat"), "w", encoding="utf8") as f:
            json.dump(
                tree_to_dict(self.tree, all_attrs=True), f, indent=2, sort_keys=True
            )

        self.logger.info("Saving the " + self.source + " tree in JSON tree format...")
        with open(self.export_path("json", "tree"), "w", encoding="utf8") as f:
            json.dump(
                tree_to_nested_dict(self.tree, all_attrs=True),
                f,
                indent=2,
                sort_keys=True,
            )


class DotExporter(BaseExporter):
    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " graph in DOT format...")
        tree_to_dot(self.tree).write(self.export_path("dot"), encoding="utf8")


class CSVExporter(BaseExporter):
    """This results in more managable CSV files, but the attributes end up as embedded JSON."""

    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " graph in CSV format...")
        df = tree_to_dataframe(self.tree, all_attrs=True)
        df.to_csv(self.export_path("csv"), index=False)


class FlatBaseExporter(BaseExporter):
    """
    Base class for exporters that flatten the tree into a list of dicts.

    The flattened tree is stored in self.orgs, and the set of attribute names
    is stored in self.attrib_names.

    The original node is included in the dict as "node" for reference, and is
    typically removed before exporting.
    """

    def __init__(self, logger: Logger, source: str, tree: Node, data_dir: str) -> None:
        super().__init__(logger, source, tree, data_dir)
        self.orgs_flat, self.attrib_names = self.flatten()

    def flatten(self, max_depth=None) -> Tuple[List[Dict[str, Any]], List[str]]:
        orgs: List[Dict[str, Any]] = []
        attrib_names: Set[str] = set()
        for org in levelorder_iter(self.tree, max_depth=max_depth):
            org = cast(Node, org)
            attrs = {}
            # Create a dict of attributes
            for key, value in org.describe(
                exclude_prefix="_", exclude_attributes=["name"]
            ):
                attrs[key] = value
            # Include node in the dict of attributes for reference
            flat_attrs = {
                "node": org,
                "path": org.path_name,
                "name": org.name,
            }
            # Flatten the dict of attributes
            for key, value in flatten(attrs).items():
                if isinstance(value, list):
                    value = json.dumps(value)
                if value is not None:
                    flat_attrs[key] = value
                    attrib_names.add(key)
            orgs.append(flat_attrs)
        return (orgs, ["path", "name"] + natsorted(attrib_names))

    def export(self):
        pass


class WideCSVExporter(FlatBaseExporter):
    """
    Export the flattened tree as a wide CSV file.

    This results in CSV files that only contain attribute values, but because
    of lists in the attribute data the number of columns can be very large.
    """

    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " tree in wide CSV format...")
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
                if skip is False:
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


class NetworkXBaseExporter(FlatBaseExporter):
    """Base class for exporters that use NetworkX to build a graph."""

    def __init__(self, logger: Logger, source: str, tree: Node, data_dir: str) -> None:
        super().__init__(logger, source, tree, data_dir)
        self.G = self.build_graph()

    def build_graph(self) -> DiGraph:
        G = nx.DiGraph()
        for org in self.orgs_flat:
            node = cast(Node, org["node"])
            del org["node"]
            if node.is_root:
                G.add_node(node.path_name, **org)
            else:
                G.add_node(node.path_name, **org)
                parent = cast(Node, node.parent)
                G.add_edge(node.path_name, parent.path_name)
        return G

    def export(self):
        pass


class GEXFExporter(NetworkXBaseExporter):
    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " graph in GEXF format...")
        nx.write_gexf(self.G, self.export_path("gexf"))
        # Update the file to remove the  lastmodifieddate attribute, which generates spurious diffs
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        with fileinput.input(self.export_path("gexf"), inplace=True) as file:
            for line in file:
                line = line.replace(' lastmodifieddate="' + date + '"', "")
                print(line, end="")


class GraphMLExporter(NetworkXBaseExporter):
    def export(self) -> None:
        self.logger.info("Saving the " + self.source + " graph in GraphML format...")
        nx.write_graphml(self.G, self.export_path("graphml"))


class CytoscapeJSONExporter(NetworkXBaseExporter):
    def export(self) -> None:
        self.logger.info(
            "Saving the " + self.source + " graph in Cytoscape JSON format..."
        )
        with open(self.export_path("cyjs"), "w", encoding="utf8") as f:
            json.dump(
                nx.cytoscape_data(self.G)["elements"], f, indent=2, sort_keys=True
            )
