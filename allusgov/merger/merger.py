from logging import Logger
from typing import Dict, List, Tuple, cast

import polars as pl
from bigtree import levelorder_iter
from bigtree.node.node import Node
from rapidfuzz import process, utils

from ..utils.utils import full_name


class Merger:
    """
    Merge class for merging two trees based on a similarity metric.

    Attributes:
        logger (logging.Logger): Logger object for logging messages.
        base_tree (Node): Base tree to be merged with.
        base_name (str): Source name for the base tree.
        source_tree (Node): Source tree to be merged.
        source_name (str): Source name for the source tree.
    """

    def __init__(
        self,
        logger: Logger,
        base_tree: Node,
        base_name: str,
        source_tree: Node,
        source_name: str,
        threshold: int,
    ) -> None:
        self.logger = logger
        self.base_tree = base_tree
        self.base_name = base_name
        self.source_tree = source_tree
        self.source_name = source_name
        self.threshold = threshold
        self.source_names = self.name_list(self.source_tree, self.source_name)
        self.base_names = self.name_list(self.base_tree, self.base_name)
        self.similarity = self.calculate_similarity()

    def name_list(self, tree: Node, source_name: str) -> Dict[str, List[Node]]:
        """
        Generate a dictionary of names and their corresponding nodes in a tree.

        Args:
            tree (Node): Tree to extract names from.
            source_name (str): Source name for the tree.

        Returns:
            dict: Dictionary of names and their corresponding nodes.
        """
        names: Dict[str, List[Node]] = {}
        for org in levelorder_iter(tree):
            org = cast(Node, org)
            name = full_name(org, source_name)
            if name not in names:
                names[name] = []
            names[name].append(org)
        return names

    def calculate_similarity(self) -> pl.DataFrame:
        """
        Calculate string similarity for the source tree against the base tree.

        Returns:
            pl.DataFrame: Similarity dataframe.
        """
        self.logger.info(
            f"Calculating string similarity for {self.source_name} against the base tree..."
        )
        matrix = process.cdist(
            self.source_names, self.base_names, processor=utils.default_process
        )
        similarity = pl.DataFrame(
            matrix, schema=list(self.base_names.keys())
        ).transpose(
            include_header=True,
            header_name="base",
            column_names=list(self.source_names.keys()),
        )
        return similarity

    def get_candidates(self, source_org_name: str) -> Dict[Node, float]:
        """
        Get candidates for a given source organization name.

        Args:
            source_org_name (str): Source organization name.
            similarity (polars.DataFrame): Similarity DataFrame.

        Returns:
            dict: Dictionary of candidate base organizations and their scores.
        """
        candidates = {}
        matches = (
            # Return up to 5 matches with a score greater than 80% of the threshold.
            self.similarity.select(["base", source_org_name])
            .filter(pl.col(source_org_name) > (self.threshold * 0.8))
            .sort([source_org_name, "base"], descending=True)
            .head(5)
        )
        for match in matches.rows():  # pylint: disable=not-an-iterable
            base = match[0]
            score = match[1]
            base_orgs = self.base_names[base]
            for base_org in base_orgs:
                candidates[base_org] = score
        return candidates

    def process_candidates(
        self, candidates: Dict[Node, float], source_org: Node
    ) -> Tuple[Node, float]:
        """
        Process candidates for a given source organization, incorparating parent scores.

        This handles the common case where different offices or programs will have identical (or nearly identical)
        names across multiple places in the tree. Adding in the parent scores makes it more likely we will merge
        the organization in at the right location.

        Args:
            candidates (dict): Dictionary of candidate base organizations and their scores.
            source_org (Node): Source organization node.

        Returns:
            Node: Selected base organization to merge.
            float: Score of the selected base organization.
        """
        for base_org, score in candidates.items():
            if source_org.is_root or base_org.is_root:
                # We are already at the root, so no parents to check.
                continue
            # Set initial values.
            current_source_org = cast(Node, source_org.parent)
            current_base_org = cast(Node, base_org.parent)
            # TODO: Split and equal factor among all parents, to favor matches nearer the root.
            factor = 0.5
            self.logger.debug(
                f"{candidates[base_org]:.1f}: candidate: "
                + f"{current_source_org.node_name} & {current_base_org.node_name}"
            )
            while (not current_source_org.is_root) and (not current_base_org.is_root):
                # Until one of the trees reaches the root, keep going up.
                factor = factor * 0.5
                # Look up the score between the current pair of orgs.
                current_source_org_name = full_name(
                    current_source_org, self.source_name
                )
                current_base_org_name = full_name(current_base_org, self.base_name)
                parent_score = (  # pylint: disable-next=unsubscriptable-object
                    self.similarity.select(["base", current_source_org_name])
                    .filter(pl.col("base") == current_base_org_name)
                    .head(1)
                    .rows()[0][1]
                )
                # Add this score to the candidate score, weighted by the factor.
                candidates[base_org] = (
                    candidates[base_org] + (parent_score * factor)
                ) / (1 + factor)
                self.logger.debug(
                    f"{candidates[base_org]:.1f}: adding score {parent_score:.1f} at factor {factor:.1f} "
                    + f"for parents {current_source_org_name} & {current_base_org_name}"
                )
                # Set the current orgs to the next level of parents.
                current_source_org = cast(Node, current_source_org.parent)
                current_base_org = cast(Node, current_base_org.parent)

        # Select the candidate with the highest score.
        selection = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[0][0]
        score = candidates[selection]

        return selection, score

    def merge(self) -> Node:
        """
        Merge the source tree into the base tree based on string similarity.

        Returns:
            Node: Merged base tree.
        """
        self.logger.info(
            f"Checking for {self.source_name} matches against the base tree..."
        )
        source_orgs = [
            cast(Node, source_org) for source_org in levelorder_iter(self.source_tree)
        ]
        source_orgs.reverse()

        for source_org in source_orgs:
            source_org_name = full_name(source_org, self.source_name)
            candidates = self.get_candidates(source_org_name)
            if len(candidates) == 0:
                self.logger.debug(f"No candidates for {source_org_name}")
                continue

            self.logger.debug(f"Checking {len(candidates)} for {source_org_name}")
            selection, score = self.process_candidates(candidates, source_org)

            if score > self.threshold:
                self.logger.info(
                    f"{score:.1f}: Selected candidate {selection.path_name} for {source_org.path_name}"
                )
                # Merge attributes to base tree.
                selection.set_attrs(
                    {self.source_name: source_org.get_attr(self.source_name)}
                )
                # Merge children
                for child in source_org.children:
                    self.logger.debug(
                        f"Merging child {child.path_name} into {selection.path_name}"
                    )
                    child.parent = selection
            else:
                self.logger.debug(
                    f"{score:.1f}: Skipped candidate {selection.path_name} for {source_org.path_name}"
                )

        return self.base_tree
