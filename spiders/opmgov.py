import scrapy


class OpmgovSpider(scrapy.Spider):
    name = "opmgov"
    allowed_domains = ["opm.gov"]
    start_url = (
        "https://www.opm.gov/about-us/open-government/Data/Apps/agencies/agencies.xml"
    )

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(self, response):
        for agency in response.xpath("//agency"):
            parent = agency.xpath("name/text()").get()
            if parent is None:
                parent = agency.xpath("dod_aggregate/text()").get().replace("D+-", "")
            yield {
                "parent": parent,
                "type": agency.xpath("type/text()").get(),
                "name": agency.xpath("agency_subelement/text()").get(),
                "employment": agency.xpath("employment/text()").get(),
            }
