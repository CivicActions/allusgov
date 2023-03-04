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
        for row in csv.DictReader(response.text.splitlines()):
            item = {}
            for key, value in row.items():
                if key == "Agency":
                    item["parent"] = value
                elif key == "Organization":
                    item["name"] = value
                else:
                    item[key.lower().replace(" ", "_")] = value
            yield item
