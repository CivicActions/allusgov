import json
from thefuzz import process, fuzz
import csv
from pprint import pprint
from collections import OrderedDict
from operator import getitem
import urllib.request

def load(source):
    with open("out/" + source + ".json", "r") as file:
        opmgov = json.load(file)

    flat = {}
    for agency in opmgov:
        name = agency["name"]
        flat[name] = {}
        for key, value in agency.items():
            flat[name][source + "_" + key] = value
    output = {}
    for agency in flat.values():
        output[path(output, source, agency)] = agency
    return output


def path(agencies, source, agency):
    name = agency[source + "_name"]
    if source + "_parent" in agency and agency[source + "_parent"] is not None and agency[source + "_parent"] != name:
        parent = agency[source + "_parent"]
        if parent in agencies:
            return path(agencies, source, agencies[parent]) + " > " + name
        else:
            return parent + " > " + name
    else:
        return name

def merge(output, source, agencies, threshold):
    output_paths = list(output.keys())
    agencies_paths = list(agencies.keys())
    for path in agencies_paths:
        matched = process.extractOne(path, output_paths, scorer=fuzz.token_sort_ratio)
        if matched[1] >= threshold:
            # print(str(matched[1]) + "|" + path + "|" + str(matched[0]))
            match = matched[0]
            output[match].update(agencies[path])
            output[match][source + "_match_threshold"] = matched[1]
            # Remove the matched item from the list of unmatched items.
            output_paths.remove(match)
            del(agencies[path])
    # Add the remaining unmatched items.
    for path, agency in agencies.items():
        output[path] = agency
    return output

def flatten(agency):
    values = {}
    # Flatten nested dictionaries (including lists of dictionaries)
    for key, value in agency.items():
        if isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    for k, v in item.items():
                        values[key + "_" + str(idx) + "_" + k] = v
                else:
                    values[key + "_" + str(idx)] = item
        elif isinstance(value, dict):
            for k, v in value.items():
                values[key + "_" + k] = v
        else:
            values[key] = value
    return values

def main():
    output = load("usagov")

    cisagov = load('cisagov')
    output = merge(output, 'cisagov', cisagov, 85)

    # We sort the OPM list by employment size, so that we can match the largest agencies first.
    opmgov = OrderedDict(sorted(load('opmgov').items(),
                                      key=lambda item: int(item[1]["opmgov_employment"]), reverse=True))
    output = merge(output, 'opmgov', opmgov, 88)

    # Write nested JSON output.
    with open("out/merged.json", "w") as jsonfile:
        json.dump(output, jsonfile, indent=4)

    # Generate flattened CSV output.
    headers = set({})
    flat = []
    for agency in output.values():
        values = flatten(agency)
        flat.append(values)
        headers.update(set(values.keys()))
    headers = list(headers)
    headers.sort()

    # Write CSV output.
    with open("out/merged.csv", "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(flat)


if __name__ == "__main__":
    main()
