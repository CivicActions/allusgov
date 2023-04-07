import json
from logging import Logger
from typing import Any, Dict, List

from bigtree import Node, nested_dict_to_tree


class Importer:
    """An importer for handling general hierarchical data."""

    root = "US FEDERAL GOVERNMENT"

    def __init__(self, logger: Logger, source_name: str, data_dir: str) -> None:
        self.logger = logger
        self.source_name = source_name
        self.data_dir = data_dir

    def load_data(self) -> List[Dict]:
        """
        Load JSON data from a file.

        Args:
            source_name (str): The name of the file (without .json extension) to load the data from.

        Returns:
            List[Dict]: A list of dictionaries representing the data.
        """
        with open(
            f"{self.data_dir}/{self.source_name}/raw.json", "r", encoding="utf-8"
        ) as file:
            return json.load(file)

    def build_tree(
        self, ids: Dict, attributes: Dict, target_id: str, source_name: str
    ) -> List:
        """
        Recursively build a tree from the given data.

        Args:
            ids (Dict): A dictionary that maps ids to their parent ids.
            attributes (Dict): A dictionary that maps ids to their attributes.
            target_id (str): The id of the target node.
            source_name (str): The name of the data source.

        Returns:
            List: A list of child dictionaries representing the tree structure.
        """
        children = []
        for item_id, parent in ids.items():
            if parent == target_id:
                child = {source_name: attributes[item_id]}
                # Prefix the name (which is the node name here) with the source name
                child["name"] = "[" + source_name + "] " + child[source_name]["name"]
                # If the ID is not the same as the name, append the ID to the name
                if child[source_name]["name"] != item_id:
                    child["name"] = child["name"] + " (" + item_id + ")"
                child["children"] = self.build_tree(
                    ids, attributes, item_id, source_name
                )
                children.append(child)
        return children

    def build(self) -> Node:
        """
        Load a tree from the given source.

        Returns:
            Node: A tree represented as nested Node objects.
        """
        data = self.load_data()
        ids = {}
        attributes: Dict[str, Dict[str, Any]] = {}
        for item in data:
            key = "name"
            parent_key = "parent"
            if "id" in item:
                key = "id"
                parent_key = "parent_id"
            if item[key] in ids:
                self.logger.warning(
                    "Duplicate %s for %s in source %s, skipping",
                    key,
                    item[key],
                    self.source_name,
                )
                continue
            if "name" not in item or item["name"] == "" or item["name"] is None:
                self.logger.warning(
                    "Item %s in source %s has no name field, skipping",
                    item[key],
                    self.source_name,
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
            self.source_name: {
                "name": self.root,
            },
            "children": self.build_tree(ids, attributes, self.root, self.source_name),
        }
        return nested_dict_to_tree(tree_dict)
