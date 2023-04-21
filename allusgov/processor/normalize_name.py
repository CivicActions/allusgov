import json
import os
import re
from logging import Logger

from bigtree import Node
from nltk.tokenize.treebank import TreebankWordDetokenizer, TreebankWordTokenizer

from .processor import Processor


class NormalizeName(Processor):
    """Normalize the name of an organization node."""

    def __init__(
        self,
        logger: Logger,
        source: str,
        data_dir: str,
    ) -> None:
        self.logger = logger
        self.source = source
        self.data_dir = data_dir

        # This is borrowed from https://github.com/ppannuto/python-titlecase/blob/main/titlecase/__init__.py#L28
        small_words = (
            r"a|an|and|as|at|but|by|en|for|if|in|of|on|or|the|to|v\.?|via|vs\.?"
        )
        self.small_re = re.compile(r"^(" + small_words + r")$", re.IGNORECASE)
        self.acronyms = self.acronym_list()
        self.words = self.word_list(self.acronyms)
        self.all_words = self.acronyms | self.words

    def word_list(self, acronyms):
        """
        Return the list of words used to fix capitalization.

        We generate this list from a few sources, rather than use a standard dictionary
        as it is important we exclude acronyms.
        """

        words_dir = os.path.dirname(os.path.abspath(__file__)) + "/words/"
        # Sourced from: http://wordlist.aspell.net/12dicts/
        with open(words_dir + "2of12inf.txt", encoding="utf-8") as f:
            words = set(f.read().splitlines())

        # Sourced from:
        # curl https://raw.githubusercontent.com/grammakov/USA-cities-and-states/master/us_cities_states_counties.csv \
        #   | xsv select -d'|' City,'State full',County,'City alias' | xsv flatten -s '' | cut -c13- \
        #   | tr '[:upper:]' '[:lower:]' | sort -u | sed -r '/.{4}/!d' > place-names.txt
        # We exclude very short place names, since they may overlap with acronyms.
        with open(words_dir + "place-names.txt", encoding="utf-8") as f:
            words.update(f.read().splitlines())

        # Sourced from:
        # curl https://www.galbithink.org/names/ginap.txt | tail -n +23 | xsv select 3-4 | xsv flatten -s '' | cut -c10- | \
        #   tr '[:upper:]' '[:lower:]' | sort -u | sed -r '/.{4}/!d' > person-names.txt
        # We exclude very short place names, since they may overlap with acronyms.
        with open(words_dir + "person-names.txt", encoding="utf-8") as f:
            words.update(f.read().splitlines())
        return words - acronyms

    def acronym_list(self):
        """Load a set of known acronyms."""
        # Remove known acronyms
        files = [
            "data.json",
            "acronyms.json",
        ]
        acronyms = set()
        for file in files:
            if os.path.exists(f"{self.data_dir}/acronyms/{file}"):
                # Include database if available
                with open(f"{self.data_dir}/acronyms/{file}", encoding="utf-8") as f:
                    for acronym in json.load(f).keys():
                        acronyms.add(acronym.lower())
        return acronyms

    def fix_whitespace(self, org_name):
        """
        Manage whitespace in the name.

        We add spaces around dashes and slashes, and remove any duplicate, leading and trailing whitespace.
        """
        return " ".join(
            org_name.replace("-", " - ").replace("/", " / ").split()
        ).strip()

    def fix_capitalization(self, tokens):
        """Fix capitalization of words in the dictionary."""
        for i, word in enumerate(tokens):
            if word.lower() in self.words:
                tokens[i] = word.title()

            if self.small_re.match(word) and i > 0:
                # Lowercase small words, except the first word.
                tokens[i] = word.lower()
        return tokens

    def fix_split_word_acronyms(self, word, character):
        """
        Acronyms occur in hyphenated ot slash-separated words.

        If word contains a hyphen or slash, split it up using regexp, fix acronyms with sub-tokens,
        then rejoin using the original symbol.
        """
        if character in word:
            sub_tokens = self.fix_embedded_acronyms(word.split(character))
            word = character.join(sub_tokens)
        return word

    def fix_embedded_acronyms(self, tokens):
        """Delete acronyms at beginning or end of string that refer to the organization name itself."""
        initials = ""
        first = ""
        last = ""
        first_index = None
        last_index = None
        for i, word in enumerate(tokens):
            word = self.fix_split_word_acronyms(word, "-")
            word = self.fix_split_word_acronyms(word, "/")
            is_caps = re.match(r"[A-Z]{2,}", word)
            if is_caps:
                if first_index is None:
                    first = word
                    first_index = i
                last = word
                last_index = i
            if len(word) > 0 and word[0].isupper():
                initials = initials + word[0]
        if len(initials) <= 1:
            return tokens
        if first_index is not None and first == initials[1:]:
            # Delete the first word
            del tokens[first_index]
        if last_index is not None and last == initials[:-1]:
            # Delete the last word
            del tokens[last_index]
        return tokens

    def process(self, node: Node) -> Node:
        attrs = node.get_attr(self.source)
        name = attrs["name"]
        orig = attrs["name"]
        # Fix whitespace: " Dept  of Education" -> "Dept of Education"
        name = self.fix_whitespace(name)
        # Fix inversions: Education, Dept of -> Dept of Education

        # Token based fixes
        tokenizer = TreebankWordTokenizer()
        detokenizer = TreebankWordDetokenizer()
        tokens = tokenizer.tokenize(name)
        # Fix abbreviations: Dept of Education -> Department of Education
        tokens = self.fix_capitalization(tokens)
        tokens = self.fix_embedded_acronyms(tokens)
        name = detokenizer.detokenize(tokens)
        if orig != name:
            self.logger.debug(f"Updated name: {orig} -> {name}")

        # Fix leading/trailing symbols/whitespace
        # Remove duplicate names (maybe?)
        # Remove the parent name from the name, if it includes it.
        # Fix capitalization

        attrs["normalized_name"] = name
        node.set_attrs({self.source: attrs})
        return node
