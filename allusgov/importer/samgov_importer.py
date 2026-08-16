from typing import Any

from bigtree import Node, add_dict_to_tree_by_path
from loguru import logger

from allusgov.importer.importer_base import ImporterBase
from allusgov.registry.registry import IMPORTERS


@IMPORTERS.register("samgov")
class SamgovImporter(ImporterBase):
    """
    An importer for handling SAM.gov hierarchical data.

    Inherits from the Importer base class.
    """

    root = "US FEDERAL GOVERNMENT"

    def build(self, source: str) -> Node:
        """
        Load a tree from the SAM.gov data source.

        Returns:
            Node: A tree represented as nested Node objects.
        """
        data = self.load_data(source=source)
        root = Node(self.root)
        root.set_attrs({"samgov": {"name": self.root}})
        path_dict: dict[str, dict[Any, Any]] = {}
        lookup: dict[int, str] = {}
        for item in data:
            name = item["fhorgname"].strip().replace("%20", " ")
            item["name"] = name
            unique_name = item["name"] + " (" + str(item["fhorgid"]) + ")"
            lookup[item["fhorgid"]] = unique_name

        id_path: list = []
        for item in data:
            if "fhorgparenthistory" in item:
                for history in item["fhorgparenthistory"]:
                    ids = [int(_id) for _id in history["fhfullparentpathid"].split(".")]
                    id_path = list(dict.fromkeys(ids))
            else:
                id_path = [item["fhdeptindagencyorgid"]]

            path = root.node_name
            for item_id in id_path:
                if item_id in lookup:
                    name = lookup[item_id]
                    path = path + "|" + name
                else:
                    logger.warning("Can't find record for ID {}, skipping", item_id)
                    continue

            if path not in path_dict:
                path_dict[path] = {}
            path_dict[path]["samgov"] = item

        return add_dict_to_tree_by_path(root, path_dict, sep="|")
