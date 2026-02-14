"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar, cast

import networkx as nx
from bigtree import Node, levelorder_iter
from flatten_json import flatten
from natsort import natsorted
from networkx import DiGraph

from allusgov import settings
from allusgov.utils.utils import BASE_PATH


class ExporterBase(ABC):
    format_key: ClassVar[str]

    @staticmethod
    def export_path(source: str, ext: str, suffix: str | None = None) -> Path:
        file_suffix = f"-{suffix}" if suffix else ""
        data_path = BASE_PATH.parent / settings.DATA_DIR / source
        data_path.mkdir(parents=True, exist_ok=True)
        return data_path.joinpath(f"{source}{file_suffix}.{ext}")

    @abstractmethod
    def export(self, source: str, tree: Node, **kwargs: Any) -> None:
        raise NotImplementedError


class FlatBaseExporter(ExporterBase):
    """
    Base class for exporters that flatten the tree into a list of dicts.

    The flattened tree is stored in self.orgs, and the set of attribute names
    is stored in self.attrib_names.

    The original node is included in the dict as "node" for reference, and is
    typically removed before exporting.
    """

    format_key: ClassVar[str]

    @staticmethod
    def flatten(tree: Node, max_depth=None) -> tuple[list[dict[str, Any]], list[str]]:
        orgs: list[dict[str, Any]] = []
        attrib_names: set[str] = set()
        for org in levelorder_iter(tree, max_depth=max_depth):
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
        return orgs, ["path", "name"] + natsorted(attrib_names)

    def export(self, source: str, tree: Node, **kwargs: Any):
        pass


class NetworkXBaseExporter(FlatBaseExporter):
    """Base class for exporters that use NetworkX to build a graph."""

    format_key: ClassVar[str]

    def build_graph(self, tree: Node) -> DiGraph:
        orgs_flat, _ = self.flatten(tree=tree, max_depth=2)
        graph = nx.DiGraph()
        for org in orgs_flat:
            node = cast(Node, org["node"])
            del org["node"]
            if node.is_root:
                graph.add_node(node.path_name, **org)
            else:
                graph.add_node(node.path_name, **org)
                parent = cast(Node, node.parent)
                graph.add_edge(node.path_name, parent.path_name)
        return graph

    def export(self, source: str, tree: Node, **kwargs: Any):
        pass
