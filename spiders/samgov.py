import scrapy
from w3lib.url import add_or_replace_parameters, url_query_parameter
from datetime import datetime
import pprint


class SamgovSpider(scrapy.Spider):
    name = "samgov"
    allowed_domains = ["sam.gov"]
    limit = 100
    base_url = "https://api.sam.gov/prod/federalorganizations/v1/orgs"
    
    def url(self, url = base_url, params = {}):
        params.update({
            "api_key": self.settings.getdict('DOTENV')["SAM_API_KEY"],
            "limit": self.limit,
        })
        return add_or_replace_parameters(url, params)

    def start_requests(self):
        yield scrapy.Request(url=self.url(), callback=self.parse)


    def parse(self, response):
        data = response.json()
        if url_query_parameter(response.url, "offset") is None and data["totalrecords"] > self.limit:
            # No existing offset, fetch additional pages.
            for offset in range(self.limit, data["totalrecords"], self.limit):
                yield response.follow(self.url(response.url, {"offset": offset}), callback=self.parse)
        for org in data["orglist"]:
            if org["status"] == "ACTIVE" and "fhorgname" in org:
                for link in org["links"]:
                    if link["rel"] == "nextlevelchildren":
                        yield response.follow(self.url(link["href"]), callback=self.parse)
                # Extract and follow IDs from the parent history - this shouldn't be neccessary, but the API is (perhaps) inconsistent or perhaps some items have inactive parents?
                if "fhorgparenthistory" in org:
                    for history in org["fhorgparenthistory"]:
                        # Identify the most recent entry.
                        date = None
                        if date == None or datetime.strptime(history["effectivedate"], "%Y-%m-%d %H:%M") > date:
                            for id in history["fhfullparentpathid"].split('.'):
                                yield response.follow(self.url(self.base_url + "?fhorgid=" + id), callback=self.parse)
                yield org
