import scrapy
from w3lib.url import add_or_replace_parameters, url_query_parameter
import pprint


class SamgovSpider(scrapy.Spider):
    name = "samgov"
    allowed_domains = ["sam.gov"]
    limit = 100
    
    def url(self, url = "https://api.sam.gov/prod/federalorganizations/v1/orgs", params = {}):
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
                # Record the parent name and ID for convenient tree building later on.
                if "fhfullparentpathname" in org:
                    # The parent name is the last element in the dotted path.
                    org["parent"] = org["fhfullparentpathname"].split('.')[-1]
                if "fhfullparentpathid" in org:
                    # The parent id is the last element in the dotted path.
                    org["parent_id"] = org["fhfullparentpathid"].split('.')[-1]
                org["id"] = org["fhorgid"]
                org["name"] = org["fhorgname"]
                yield org
