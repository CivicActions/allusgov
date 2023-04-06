from datetime import datetime
from logging import Logger
from typing import Any, Dict

from bigtree import Node, add_dict_to_tree_by_path

from . import importer


class SamgovImporter(importer.Importer):
    """
    An importer for handling SAM.gov hierarchical data.

    Inherits from the Importer base class.
    """

    def __init__(self, logger: Logger, source_name: str, data_dir: str) -> None:
        super().__init__(logger, source_name=source_name, data_dir=data_dir)

    def build(self) -> Node:
        """
        Load a tree from the SAM.gov data source.

        Returns:
            Node: A tree represented as nested Node objects.
        """
        data = self.load_data()
        root = Node(self.root)
        root.set_attrs({"samgov": {"name": self.root}})
        path_dict: Dict[str, Dict[Any, Any]] = {}
        lookup: Dict[int, str] = {}
        for item in data:
            name = item["fhorgname"].strip().replace("%20", " ")
            item["name"] = name
            unique_name = item["name"] + " (" + str(item["fhorgid"]) + ")"
            lookup[item["fhorgid"]] = unique_name
        for item in data:
            if "fhorgparenthistory" in item:
                for history in item["fhorgparenthistory"]:
                    date = None
                    if (
                        date is None
                        or datetime.strptime(history["effectivedate"], "%Y-%m-%d %H:%M")
                        > date
                    ):
                        ids = [
                            int(id) for id in history["fhfullparentpathid"].split(".")
                        ]
                        id_path = list(dict.fromkeys(ids))
            else:
                id_path = [item["fhdeptindagencyorgid"]]

            path = root.node_name
            for item_id in id_path:
                if item_id in lookup:
                    name = lookup[item_id]
                    path = path + "|" + name
                else:
                    self.logger.warning(
                        "Can't find record for ID %s, skipping", item_id
                    )
                    continue

            if path not in path_dict:
                path_dict[path] = {}
            path_dict[path]["samgov"] = item

        return add_dict_to_tree_by_path(root, path_dict, sep="|")
