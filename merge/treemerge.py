import json
from polyfuzz import PolyFuzz
from bigtree import (
    Node,
    add_dict_to_tree_by_path,
    levelorder_iter,
    tree_to_dot,
    tree_to_dict,
    tree_to_nested_dict,
    nested_dict_to_tree,
    yield_tree,
)
import networkx as nx
import logging
import argparse
from datetime import datetime
from flatten_json import flatten


def load_data(source):
    with open("out/" + source + "/raw.json", "r") as f:
        data = json.load(f)
    return data


def base_tree(logger, root):
    data = load_data("samgov")
    root = Node(root)
    path_dict = {}
    lookup = {}
    # Create a lookup so we can ensure we use the canonical name for each org.
    for item in data:
        # This does a little cleanup, some items have trailing whitespace or %20 instead of spaces.
        lookup[item["fhorgid"]] = item["fhorgname"].strip().replace("%20", " ")
    for item in data:
        # Record the parent name and ID for convenient tree building later on.
        if "fhorgparenthistory" in item:
            for history in item["fhorgparenthistory"]:
                # Identify the most recent entry.
                date = None
                if (
                    date == None
                    or datetime.strptime(history["effectivedate"], "%Y-%m-%d %H:%M")
                    > date
                ):
                    # Deduplicate repeated elements on the path.
                    ids = [int(id) for id in history["fhfullparentpathid"].split(".")]
                    id_path = list(dict.fromkeys(ids))
        else:
            # If there is no history or path, we assume the agency is the parent.
            id_path = [item["fhdeptindagencyorgid"]]

        # Generate a string path from the ID path, using the lookup table.
        path = root.node_name
        seen = set()
        for id in id_path:
            if id in lookup:
                name = lookup[id]
                # Disambiguate duplicate names in the same tree span.
                if name in seen:
                    name = name + " (" + item["fhorgtype"] + ")"
                    item["disambiguated"] = True
                seen.add(name)
                path = path + "|" + name
            else:
                logger.warning("Can't find record for ID %s, skipping", id)
                continue

        # Add the item to the path dictionary.
        if path not in path_dict:
            path_dict[path] = {}
        path_dict[path]["samgov"] = item

    # Generate the tree from the path dictionary.
    return add_dict_to_tree_by_path(root, path_dict, sep="|")


def export_path(source, ext, suffix=""):
    if suffix != "":
        suffix = "-" + suffix
    return "out/" + source + "/" + source + suffix + "." + ext


def export(logger, source, tree):
    logger.info("Saving the " + source + " graph in text format...")
    with open(export_path(source, "txt"), "w") as f:
        for branch, stem, node in yield_tree(tree):
            print(f"{branch}{stem}{node.node_name}", file=f)

    logger.info("Saving the " + source + " graph in JSON flat format...")
    with open(export_path(source, "json", "flat"), "w") as f:
        json.dump(tree_to_dict(tree, all_attrs=True), f)

    logger.info("Saving the " + source + " graph in JSON tree format...")
    with open(export_path(source, "json", "tree"), "w") as f:
        json.dump(tree_to_nested_dict(tree, all_attrs=True), f)

    logger.info("Saving the " + source + " graph in DOT format...")
    tree_to_dot(tree).write_dot(export_path(source, "dot"))

    G = nx.DiGraph()
    for node in levelorder_iter(tree):
        # Graph formats don't support nested attributes, so generate a flattened list.
        attrs = {}
        for key, value in node.describe(
            exclude_prefix="_", exclude_attributes=["name"]
        ):
            attrs[key] = value
        flat_attrs = {}
        for key, value in flatten(attrs).items():
            # Flatten lists to JSON also.
            if type(value) == list:
                value = json.dumps(value)
            # Skip empty attributes.
            if value is not None:
                flat_attrs[key] = value
        if node.is_root:
            root = node.path_name
            G.add_node(node.path_name, **flat_attrs)
        else:
            G.add_node(node.path_name, **flat_attrs)
            G.add_edge(node.path_name, node.parent.path_name)

    logger.info("Saving the " + source + " graph in GEXF format...")
    nx.write_gexf(G, export_path(source, "gexf"))

    logger.info("Saving the " + source + " graph in GraphML format...")
    nx.write_graphml(G, export_path(source, "graphml"))

    logger.info("Saving the " + source + " graph in Cytoscape JSON format...")
    with open(export_path(source, "cyjs"), "w") as f:
        json.dump(nx.cytoscape_data(G)["elements"], f)


def build_tree(ids, attributes, target_id):
    children = []
    names = set()
    duplicates = []
    # Find duplicate names, so we can disambiguate them.
    for id, parent in ids.items():
        name = attributes[id]["name"]
        if parent == target_id:
            if name in names:
                duplicates.append(name)
            names.add(name)
    # Build the tree.
    for id, parent in ids.items():
        if parent == target_id:
            child = attributes[id]
            # Disambiguate duplicate names.
            if child["name"] in duplicates:
                child["name"] = child["name"] + " (" + id + ")"
                child["disambiguated"] = True
            child["children"] = build_tree(ids, attributes, id)
            children.append(child)
    return children


def load_tree(logger, root, source):
    data = load_data(source)
    ids = {}
    attributes = {}
    for item in data:
        key = "name"
        parent_key = "parent"
        if "id" in item:
            key = "id"
            parent_key = "parent_id"
        if item[key] in ids:
            logger.warning(
                "Duplicate %s for %s in source %s, skipping", key, item[key], source
            )
            continue
        if "name" not in item or item["name"] == "" or item["name"] == None:
            logger.warning(
                "Item %s in source %s has no name field, skipping", item[key], source
            )
            continue
        # Set parent to root if not specified.
        if parent_key not in item or item[parent_key] == "" or item[parent_key] == None:
            item[parent_key] = root
        ids[item[key]] = item[parent_key]
        # Build an attribute dictionary, excluding parent attributes as these are now embedded in the tree.
        attributes[item[key]] = {}
        for attribute, value in item.items():
            if attribute not in ["parent", "parent_id"]:
                attributes[item[key]][attribute] = value

    tree_dict = {
        "name": root,
        "children": build_tree(ids, attributes, root),
    }
    # pprint.pprint(tree_dict, sort_dicts=False)
    return nested_dict_to_tree(tree_dict)


def main():
    parser = argparse.ArgumentParser(
        description="Merge federal government data from multiple sources."
    )
    parser.add_argument(
        "--log-level",
        default="normal",
        choices=["quiet", "normal", "debug"],
        help="Log level",
    )
    parser.add_argument("--export", action="store_true")
    args = parser.parse_args()

    log_level = {
        "quiet": logging.ERROR,
        "normal": logging.INFO,
        "debug": logging.DEBUG,
    }[args.log_level]
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    # Construct the base tree
    root = "US FEDERAL GOVERNMENT"
    logger.info("Constructing the base tree...")
    tree = base_tree(logger, root)
    if args.export:
        logger.info("Exporting the base tree...")
        export(logger, "samgov", tree)

    sources = ["budget", "cisagov", "opmgov", "usagov", "usaspending"]
    # sources = ["cisagov", "opmgov", "usagov", "usaspending"]
    for source in sources:
        logger.info("Constructing the " + source + " tree...")
        source_tree = load_tree(logger, root, source)
        if args.export:
            logger.info("Exporting the " + source + " tree...")
            export(logger, source, source_tree)


if __name__ == "__main__":
    main()
