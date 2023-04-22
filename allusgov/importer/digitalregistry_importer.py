from bigtree import Node
from .importer import Importer


class DigitalRegistryImporter(Importer):
    """Wraps the normal importer to handle digitalregistry data that comes in an inverted format."""

    def clean_dict(self, d):
        cleaned_dict = {}
        for key, value in d.items():
            if isinstance(value, dict):
                cleaned_value = self.clean_dict(value)
                if cleaned_value:  # Check if the cleaned dict is not empty
                    cleaned_dict[key] = cleaned_value
            elif value is not None:  # Ignore empty values (None)
                cleaned_dict[key] = value
        return cleaned_dict

    def build(self) -> Node:
        output = {}
        for item in self.data:
            item_type = item["type"]
            ids = []
            # Extract and initialize agencies
            for agency in item["attributes"]["agencies"]:
                if agency["id"] not in output:
                    output[agency["id"]] = self.clean_dict(agency)
                    output[agency["id"]]["parent"] = None
                    ids.append(agency["id"])
                if item_type not in output[agency["id"]]:
                    output[agency["id"]][item_type] = []
            del item["attributes"]["agencies"]
            # Clean up null values
            attributes = self.clean_dict(item["attributes"])
            # Clean up tags data
            tags = []
            for tag in attributes["tags"]:
                tags.append(tag["name"])
            attributes["tags"] = tags
            # Add the item to each agency
            for agency_id in ids:
                output[agency_id][item_type].append(attributes)
        self.data = []
        for agency in output.values():
            self.data.append(agency)
        return Importer.build(self)
