import scrapy
import csv


class CisagovSpider(scrapy.Spider):
    name = "cisagov"
    start_url = "https://raw.githubusercontent.com/cisagov/dotgov-data/main/current-federal.csv"

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(self, response):
        items = {}
        for row in csv.DictReader(response.text.splitlines()):
            item = {}
            for key, value in row.items():
                item[key.lower().replace(" ", "_")] = value
            name = item["organization"]
            parent = item["agency"]
            # If an organization is its own parent, then it is a top-level organization.
            if name == parent:
                parent = None
            if name not in items:
                items[name] = {"name": name, "parent": parent, "records": []}
            items[name]["records"].append(item)
        for item in items.values():
            yield item
