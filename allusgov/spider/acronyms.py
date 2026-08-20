import os
import tempfile
import warnings
from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

import pandas as pd
import scrapy
import tabula
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning
from scrapy.http.request import Request
from scrapy.http.response import Response


class GovSpeakAcronymsSpider(scrapy.Spider):
    name = "govspeak"
    allowed_domains = ["ucsd.libguides.com"]
    start_url = "http://ucsd.libguides.com"

    async def start(self) -> AsyncIterator[Request]:
        yield scrapy.Request(
            url=f"{self.start_url}/govspeak/pagea",
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def parse(self, response: Response, **kwargs) -> Iterator[dict[str, Any]]:
        pages = response.css(".nav > li")
        for page in pages:
            yield response.follow(
                page.css("a::attr(href)").get(),
                callback=self.parse,
                headers={"User-Agent": "Mozilla/5.0"},
            )
        rows = response.xpath("//table//tr")

        for row in rows:
            acronym = row.xpath("td[1]/text()").get()
            expansions_raw = row.xpath("td[2]").get()

            # Use BeautifulSoup to parse the content of the <td> tag
            soup = BeautifulSoup(expansions_raw, "html.parser")

            # Get the content of the <td> tag without the opening and closing tags
            expansions_raw = "".join(str(e) for e in soup.td.contents)
            expansions_raw = expansions_raw.split("|")
            expansions = []

            for expansion_text in expansions_raw:
                # Since we are splitting on strings, we use BeautifulSoup to reparse HTML fragments
                with warnings.catch_warnings():
                    warnings.simplefilter(
                        "ignore", category=MarkupResemblesLocatorWarning
                    )
                    soup_def = BeautifulSoup(expansion_text, "html.parser")

                # Get the text and link only in the actual expansion (before the i tag)
                expansion = (
                    soup_def.get_text(strip=True, separator="|")
                    .split("|", maxsplit=1)[0]
                    .strip()
                )
                expansion_link = None
                if soup_def.a and not soup_def.a.find_parent("i"):
                    expansion_link = soup_def.a["href"]

                notes = []
                note_section = soup_def.find("i")
                if note_section:
                    note_section_content = "".join(
                        str(e) for e in note_section.contents
                    ).split(";")
                    for note_text in note_section_content:
                        with warnings.catch_warnings():
                            warnings.simplefilter(
                                "ignore", category=MarkupResemblesLocatorWarning
                            )
                            note_soup = BeautifulSoup(note_text, "html.parser")
                        note_text = note_soup.get_text().strip(" ()")
                        note_link = note_soup.a["href"] if note_soup.a else None
                        note_dict = {"note": note_text}
                        if note_link:
                            note_dict["link"] = note_link
                        notes.append(note_dict)

                expansion_dict = {"expansion": expansion, "source": "govspeak"}
                if expansion_link:
                    expansion_dict["link"] = expansion_link
                if notes:
                    expansion_dict["notes"] = notes
                expansions.append(expansion_dict)
            if acronym and expansions:
                yield {"acronym": acronym, "expansions": expansions}


class DoDAcronymsSpider(scrapy.Spider):
    name = "dod"
    allowed_domains = ["jsouapplicationstorage.blob.core.windows.net"]
    start_url = "https://jsouapplicationstorage.blob.core.windows.net"

    async def start(self) -> AsyncIterator[Request]:
        yield scrapy.Request(
            url=f"{self.start_url}/press/560/DoD%20Dictionary%20of%20Military%20"
            f"and%20Associated%20Terms%20JUNE%2025.pdf",
            callback=self.parse,
        )

    @staticmethod
    def parse(response: Response, **kwargs) -> Iterator[dict[str, Any]]:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as file:
            file.write(response.body)
            pdf_path = file.name

        xmin = 90
        ymax = 752
        xmax = 600
        first_page = 195
        title_pages = [
            195,
            203,
            205,
            215,
            221,
            225,
            229,
            233,
            235,
            239,
            247,
            249,
            251,
            257,
            263,
            267,
            271,
            273,
            275,
            281,
            285,
            291,
            293,
            295,
            297,
        ]
        blank_pages = [
            202,
            214,
            224,
            228,
            232,
            246,
            248,
            262,
            266,
            272,
            280,
            288,
            289,
            290,
            292,
        ]
        acronyms: dict[str, list[dict[str, str]]] = {}
        try:
            for page in range(195, 297):
                ymin = 72
                if page in blank_pages:
                    continue
                if page == first_page:
                    ymin = 173
                elif page in title_pages:
                    ymin = 102
                dfs = tabula.read_pdf(
                    pdf_path,
                    pages=page,
                    area=(ymin, xmin, ymax, xmax),
                )
                for df in dfs:
                    df = cast(pd.DataFrame, df)
                    for index, row in df.iterrows():
                        if pd.isna(row[0]):
                            continue
                        acronym = row[0]
                        expansion = row[1]
                        j = index + 1
                        try:
                            while pd.isna(df.iloc[j, 0]):
                                expansion += " " + df.iloc[j, 1]
                                j += 1
                        except IndexError:
                            pass
                        if acronym not in acronyms:
                            acronyms[acronym] = []
                        expansions = expansion.split(";")
                        for expansion in expansions:
                            acronyms[acronym].append(
                                {"expansion": expansion.strip(), "source": "dod"}
                            )
        finally:
            os.unlink(pdf_path)
        for acronym, expansions in acronyms.items():
            yield {"acronym": acronym, "expansions": expansions}
