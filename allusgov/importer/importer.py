"""
Copyright 2019-2026 CivicActions, Inc. See the README file at the top-level
directory of this distribution and at https://github.com/CivicActions/allusgov#license.
"""

from typing import Any

from bigtree import Node, nested_dict_to_tree
from loguru import logger

from allusgov.models.importer_base import ImporterBase
from allusgov.models.registry import IMPORTERS


@IMPORTERS.register("importer")
class Importer(ImporterBase):
    """An importer for handling general hierarchical data."""

    root = "US FEDERAL GOVERNMENT"

    def build_tree(
        self, ids: dict, attributes: dict, target_id: str, source_name: str
    ) -> list:
        """
        Recursively build a tree from the given data.

        :param ids: A dictionary that maps ids to their parent ids.
        :param attributes: A dictionary that maps ids to their attributes.
        :param target_id: The id of the target node.
        :param source_name: The name of the data source.
        :returns: A list of child dictionaries representing the tree structure.
        """

        children = []
        for item_id, parent in ids.items():
            if parent == target_id:
                child = {source_name: attributes[item_id]}
                # Prefix the name (which is the node name here) with the source name
                child["name"] = "[" + source_name + "] " + child[source_name]["name"]
                # If the ID is not the same as the name, append the ID to the name
                if child[source_name]["name"] != item_id:
                    child["name"] = child["name"] + " (" + str(item_id) + ")"
                child["children"] = self.build_tree(
                    ids, attributes, item_id, source_name
                )
                children.append(child)
        return children

    def build(self, source: str) -> Node:
        """
        Load a tree from the given source.

        :returns: A tree represented as nested Node objects.
        """
        raw_data = self.load_data(source=source)
        ids: dict = {}
        attributes: dict[str, dict[str, Any]] = {}
        for item in raw_data:
            key = "name"
            parent_key = "parent"
            if "id" in item:
                key = "id"
                parent_key = "parent_id"
            if item[key] in ids:
                logger.warning(
                    "Duplicate %s for %s in source %s, skipping",
                    key,
                    item[key],
                    source,
                )
                continue
            if "name" not in item or item["name"] == "" or item["name"] is None:
                logger.warning(
                    "Item %s in source %s has no name field, skipping",
                    item[key],
                    source,
                )
                continue
            if (
                parent_key not in item
                or item[parent_key] == ""
                or item[parent_key] is None
            ):
                item[parent_key] = self.root
            ids[item[key]] = item[parent_key]
            attributes[item[key]] = {}
            for attribute, value in item.items():
                if attribute not in ["parent", "parent_id"]:
                    attributes[item[key]][attribute] = value

        tree_dict = {
            "name": self.root,
            source: {
                "name": self.root,
            },
            "children": self.build_tree(ids, attributes, self.root, source),
        }
        return nested_dict_to_tree(tree_dict)
