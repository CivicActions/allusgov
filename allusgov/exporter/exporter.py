import json
import networkx as nx
from bigtree import (
    levelorder_iter,
    tree_to_dict,
    tree_to_nested_dict,
    tree_to_dot,
    yield_tree,
)
from flatten_json import flatten
from ..utils.utils import full_name


class BaseExporter:
    def __init__(self, logger, source, tree, data_dir):
        self.logger = logger
        self.source = source
        self.tree = tree
        self.data_dir = data_dir

    def export_path(self, ext, suffix=""):
        if suffix != "":
            suffix = "-" + suffix
        return (
            self.data_dir + "/" + self.source + "/" + self.source + suffix + "." + ext
        )


class TextExporter(BaseExporter):
    def print_tree(self, tree, source="samgov", file=None):
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

    def export(self):
        self.logger.info("Saving the " + self.source + " graph in text format...")
        with open(self.export_path("txt"), "w") as f:
            self.print_tree(self.tree, self.source, f)


class JSONExporter(BaseExporter):
    def export(self):
        self.logger.info("Saving the " + self.source + " graph in JSON flat format...")
        with open(self.export_path("json", "flat"), "w") as f:
            json.dump(tree_to_dict(self.tree, all_attrs=True), f)

        self.logger.info("Saving the " + self.source + " graph in JSON tree format...")
        with open(self.export_path("json", "tree"), "w") as f:
            json.dump(tree_to_nested_dict(self.tree, all_attrs=True), f)


class DotExporter(BaseExporter):
    def export(self):
        self.logger.info("Saving the " + self.source + " graph in DOT format...")
        tree_to_dot(self.tree).write_dot(self.export_path("dot"))


class NetworkXBaseExporter(BaseExporter):
    def __init__(self, logger, source, tree, data_dir):
        super().__init__(logger, source, tree, data_dir)
        self.G = self.build_graph()

    def build_graph(self):
        G = nx.DiGraph()
        for org in levelorder_iter(self.tree):
            attrs = {}
            for key, value in org.describe(
                exclude_prefix="_", exclude_attributes=["name"]
            ):
                attrs[key] = value
            flat_attrs = {}
            for key, value in flatten(attrs).items():
                if type(value) == list:
                    value = json.dumps(value)
                if value is not None:
                    flat_attrs[key] = value
            if org.is_root:
                root = org.path_name
                G.add_node(org.path_name, **flat_attrs)
            else:
                G.add_node(org.path_name, **flat_attrs)
                G.add_edge(org.path_name, org.parent.path_name)
        return G


class GEXFExporter(NetworkXBaseExporter):
    def export(self):
        self.logger.info("Saving the " + self.source + " graph in GEXF format...")
        nx.write_gexf(self.G, self.export_path("gexf"))


class GraphMLExporter(NetworkXBaseExporter):
    def export(self):
        self.logger.info("Saving the " + self.source + " graph in GraphML format...")
        nx.write_graphml(self.G, self.export_path("graphml"))


class CytoscapeJSONExporter(NetworkXBaseExporter):
    def export(self):
        self.logger.info(
            "Saving the " + self.source + " graph in Cytoscape JSON format..."
        )
        with open(self.export_path("cyjs"), "w") as f:
            json.dump(nx.cytoscape_data(self.G)["elements"], f)
