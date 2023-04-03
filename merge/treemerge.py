import json
from rapidfuzz import process, utils
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
import polars as pl

pl.Config.set_fmt_str_lengths(1000)
pl.Config.set_tbl_rows(1000)

def load_data(source):
    with open("out/" + source + "/raw.json", "r") as f:
        data = json.load(f)
    return data


def base_tree(logger, root_name):
    data = load_data("samgov")
    root = Node(root_name)
    root.set_attrs({"samgov": {"name": root_name}})
    path_dict = {}
    # Create a lookup so we can ensure we use the canonical name for each org.
    lookup = {}
    for item in data:
        # This does a little cleanup, some items have trailing whitespace or %20 instead of spaces.
        name = item["fhorgname"].strip().replace("%20", " ")
        item["name"] = name
        # Geneate a unique name for each org, including the ID for the node names.
        unique_name = item["name"] + " (" + str(item["fhorgid"]) + ")"
        lookup[item["fhorgid"]] = unique_name
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
        for id in id_path:
            if id in lookup:
                name = lookup[id]
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

def print_tree(tree, source="samgov", file=None):
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

def export(logger, source, tree):
    logger.info("Saving the " + source + " graph in text format...")
    with open(export_path(source, "txt"), "w") as f:
        print_tree(tree, source, f)

    logger.info("Saving the " + source + " graph in JSON flat format...")
    with open(export_path(source, "json", "flat"), "w") as f:
        json.dump(tree_to_dict(tree, all_attrs=True), f)

    logger.info("Saving the " + source + " graph in JSON tree format...")
    with open(export_path(source, "json", "tree"), "w") as f:
        json.dump(tree_to_nested_dict(tree, all_attrs=True), f)

    logger.info("Saving the " + source + " graph in DOT format...")
    tree_to_dot(tree).write_dot(export_path(source, "dot"))

    G = nx.DiGraph()
    for org in levelorder_iter(tree):
        # Graph formats don't support nested attributes, so generate a flattened list.
        attrs = {}
        for key, value in org.describe(
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
        if org.is_root:
            root = org.path_name
            G.add_node(org.path_name, **flat_attrs)
        else:
            G.add_node(org.path_name, **flat_attrs)
            G.add_edge(org.path_name, org.parent.path_name)

    logger.info("Saving the " + source + " graph in GEXF format...")
    nx.write_gexf(G, export_path(source, "gexf"))

    logger.info("Saving the " + source + " graph in GraphML format...")
    nx.write_graphml(G, export_path(source, "graphml"))

    logger.info("Saving the " + source + " graph in Cytoscape JSON format...")
    with open(export_path(source, "cyjs"), "w") as f:
        json.dump(nx.cytoscape_data(G)["elements"], f)


def build_tree(ids, attributes, target_id, source_name):
    children = []
    # Build the tree.
    for id, parent in ids.items():
        if parent == target_id:
            # Nest attributes under a single source attribute.
            child = {source_name: attributes[id]}
            # Generate a unique node name, including the ID if it adds uniquneess.
            child["name"] = "[" + source_name + "] " + child[source_name]["name"]
            if child["name"] != id:
                child["name"] = child["name"] + " (" + id + ")"
            child["children"] = build_tree(ids, attributes, id, source_name)
            children.append(child)
    return children


def load_tree(logger, root, source_name):
    data = load_data(source_name)
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
                "Duplicate %s for %s in source %s, skipping", key, item[key], source_name
            )
            continue
        if "name" not in item or item["name"] == "" or item["name"] == None:
            logger.warning(
                "Item %s in source %s has no name field, skipping", item[key], source_name
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
        source_name: {
            "name": root,
        },
        "children": build_tree(ids, attributes, root, source_name),
    }
    return nested_dict_to_tree(tree_dict)

def full_name(org, source_name):
    # If there is a name for the requested source, return that.
    attrs = org.get_attr(source_name)
    if attrs and "name" in attrs:
        return attrs["name"]
    # Otherwise, return the first name attribute found form any source.
    attrs = org.describe(
        exclude_prefix="_", exclude_attributes=["name"]
    )
    return attrs[0][1]["name"]

def name_list(tree, source_name):
    names = {}
    for org in levelorder_iter(tree):
        name = full_name(org, source_name)
        if name not in names:
            names[name] = []
        names[name].append(org)
    return names

def merge(logger, base_tree, base_name, source_tree, source_name):
    logger.info("Calculating string similarity for " + source_name + " against the base tree...")
    # Match every element in the source tree to every element in the base tree.
    source_names = name_list(source_tree, source_name)
    base_names = name_list(base_tree, base_name)
    matrix = process.cdist(source_names, base_names, processor=utils.default_process)
    similarity = pl.DataFrame(matrix, schema=base_names.keys()).transpose(include_header=True, header_name="base", column_names=source_names.keys())
    logger.info("Checking for " + source_name + " matches against the base tree...")
    # Iterate through source nodes, starting with the deepest/leaf nodes.
    source_orgs = []
    for source_org in levelorder_iter(source_tree):
        source_orgs.append(source_org)
    source_orgs.reverse()
    for source_org in source_orgs:
        source_org_name = full_name(source_org, source_name)
        candidates = {}
        matches = similarity.select(["base", source_org_name]).sort([source_org_name, "base"], descending=True).head(5)
        for match in matches.rows():
            base = match[0]
            score = match[1]
            # Because of duplicate names, there may be multiple base orgs with the same name.
            base_orgs = base_names[base]
            for base_org in base_orgs:
                candidates[base_org] = score
        logger.debug("Checking " + str(len(candidates)) + " for " + source_org_name)
        for base_org, score in candidates.items():
            if (not source_org.is_root) and (not base_org.is_root):
                current_source_org = source_org.parent
                current_base_org = base_org.parent
                factor = 0.5
                logger.debug(str(round(candidates[base_org], 1)) + ": candidate: " + current_source_org.node_name + " & " + current_base_org.node_name)
                while (not current_source_org.is_root) and (not current_base_org.is_root):
                    factor = factor * 0.5
                    current_source_org_name = full_name(current_source_org, source_name)
                    current_base_org_name = full_name(current_base_org, base_name)
                    parent_score = similarity.select(["base", current_source_org_name]).filter(pl.col("base") == current_base_org_name).head(1).rows()[0][1]
                    candidates[base_org] = (candidates[base_org] + (parent_score * factor)) / (1 + factor)
                    logger.debug(str(round(candidates[base_org], 1)) + ": adding score " + str(round(parent_score, 1)) + " at factor " + str(round(factor, 1)) + " for parents " + current_source_org_name + " & " + current_base_org_name)
                    current_source_org = current_source_org.parent
                    current_base_org = current_base_org.parent
        selection = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[0][0]
        score = candidates[selection]
        action = "skipped"
        if score > 95:
            action = "selected"
            # Merge attributes to base tree.
            selection.set_attrs({source_name: source_org.get_attr(source_name)})
            # Merge children
            for child in source_org.children:
                logger.debug("Merging child" + child.path_name + " into " + selection.path_name)
                child.parent = selection
        logger.info(str(round(score, 1)) + ": " + action + " candidate " + selection.path_name + " for " + source_org.path_name)
    return base_tree

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
    parser.add_argument("--merge", action="store_true")
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
    base = base_tree(logger, root)
    if args.export:
        logger.info("Exporting the base tree...")
        export(logger, "samgov", base)

    sources = ["budget", "cisagov", "opmgov", "usagov", "usaspending"]
    for source_name in sources:
        logger.info("Constructing the " + source_name + " tree...")
        source = load_tree(logger, root, source_name)
        if args.export:
            logger.info("Exporting the " + source_name + " tree...")
            export(logger, source_name, source)
        if args.merge:
            logger.info("Merging in the " + source_name + " tree...")
            base = merge(logger, base, "samgov", source, source_name)
    if args.merge:
        export(logger, "merged", base)

if __name__ == "__main__":
    main()
