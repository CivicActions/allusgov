import scrapy
from io import BytesIO
import polars as pl
from itertools import chain

pl.Config.set_fmt_str_lengths(1000)
pl.Config.set_tbl_rows(1000)


class BudgetSpider(scrapy.Spider):
    name = "budget"
    # The below 2 rows will need to be updated annually.
    start_url = (
        "https://www.govinfo.gov/content/pkg/BUDGET-2024-DB/xls/BUDGET-2024-DB-2.xlsx"
    )
    data_range = "A1:CB5581"
    year = 2024

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse,
            headers={"User-Agent": "Mozilla/5.0"},
        )

    def budget_years(self):
        return [
            pl.sum(str(self.year - 5)),
            pl.sum(str(self.year - 4)),
            pl.sum(str(self.year - 3)),
            pl.sum(str(self.year - 2)),
            pl.sum(str(self.year - 1)),
            pl.sum(str(self.year)),
        ]

    # Normalizes item field names.
    def process_item(self, item, fields, parent_key, name_key):
        for key, value in fields:
            if key == parent_key:
                item["parent"] = value
            elif key == name_key:
                item["name"] = value
            else:
                item[key.lower().replace(" ", "_")] = value
        return item
        
    def agencies(self, budget):
        q = (
            budget.lazy()
            .groupby("Agency Code", "Agency Name").agg(
                self.budget_years()
            )
            # Only consider bureaus with a budget in the current year.
            .filter(pl.col(str(self.year)) > 0)
        )
        agencies = q.collect()
        for agency in agencies.iter_rows(named=True):
            yield self.process_item({"budget_level": "agency"}, agency.items(), None, "Agency Name")

    def bureaus(self, budget):
        q = (
            budget.lazy()
            .groupby("Agency Code", "Agency Name", "Bureau Code", "Bureau Name").agg(
                self.budget_years()
            )
            # Only consider bureaus with a budget in the current year.
            .filter(pl.col(str(self.year)) > 0)
        )
        bureaus = q.collect()
        for bureau in bureaus.iter_rows(named=True):
            yield self.process_item({"budget_level": "bureau"}, bureau.items(), "Agency Name", "Bureau Name")

    def accounts(self, budget):
        q = (
            budget.lazy()
            .groupby("Bureau Code", "Bureau Name", "Account Code", "Account Name").agg(
                self.budget_years()
            )
            # Only consider accounts with a budget in the current year.
            .filter(pl.col(str(self.year)) > 0)
            # Only select accounts that have some name that indicates that they are an office, program etc.
            .filter(
                pl.col("Account Name").str.contains(
                    r"(?i)\b(office|commission|council|committee|center|program|service$|institute)\b"
                )
            )
            # Exclude accounts that have the same name as the bureau.
            .filter(pl.col("Account Name") != pl.col("Bureau Name"))
        )
        accounts = q.collect().with_columns(
            # String some common financial suffixes to leave just the program name.
            pl.col("Account Name")
            .str.replace(r"(?i)\b(salaries and expenses|account|fund)$", "")
            .str.strip(" ,")
        )
        for account in accounts.iter_rows(named=True):
            yield self.process_item({"budget_level": "account"}, account.items(), "Bureau Name", "Account Name")

    def parse(self, response):
        budget = pl.read_excel(BytesIO(response.body), sheet_id=1)
        results = chain(self.agencies(budget), self.bureaus(budget), self.accounts(budget))
        for item in results:
            yield item 