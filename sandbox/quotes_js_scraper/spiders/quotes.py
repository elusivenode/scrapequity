import scrapy, os, glob
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from scrapy_playwright.page import PageMethod
from playwright._impl._errors import TargetClosedError
from scrapy.selector import Selector


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    directory = "output"
    if not os.path.exists(directory):
        os.makedirs(directory)

    files = glob.glob(os.path.join(directory, "*"))
    for f in files:
        os.remove(f)

    def start_requests(self):
        url = "https://au.finance.yahoo.com/quote/CBA.AX/history/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        yield scrapy.Request(
            url,
            headers=headers,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods=[PageMethod("wait_for_selector", "table")],
            ),
            callback=self.parse,
            errback=self.errback,
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        try:
            last_position = await page.evaluate("window.scrollY")

            while True:
                # scroll by 700 while not at the bottom
                await page.evaluate("window.scrollBy(0, 700)")
                await page.wait_for_timeout(
                    750
                )  # wait for 750ms for the request to complete
                current_position = await page.evaluate("window.scrollY")

                if current_position == last_position:
                    # print("Reached the bottom of the page.")
                    break

                last_position = current_position

        except TargetClosedError as e:
            self.logger.error(f"TargetClosedError: {e}")

        # await page.wait_for_timeout(120000)
        page_details = response.url.split("/")
        idx = page_details.index("quote")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        ticker = page_details[idx + 1]
        directory = "output"
        filename = os.path.join(
            directory, f"yahoo-finance-history-{timestamp}-{ticker}.txt"
        )
        content = await page.content()
        selector = Selector(text=content)
        html = selector.css("#Col1-1-HistoricalDataTable-Proxy").get()
        soup = BeautifulSoup(html, "html.parser")

        # Find the table with the specific class
        table = soup.find("table", {"data-test": "historical-prices"})

        # Extract all rows from the table
        rows = table.find_all("tr")

        with open(filename, "a", encoding="utf-8") as file:
            # Extract the header row
            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
            # headers = header_row.css("th::text").getall()
            file.write("|".join(headers) + "\n")  # Write headers to the file

            # Iterate over the rows and extract data
            for row in rows[1:]:  # Skip the header row
                columns = row.find_all("td")
                # columns = row.css("td::text").getall()
                if columns:  # Skip empty rows
                    data = [col.get_text(strip=True) for col in columns]
                    file.write("|".join(data) + "\n")  # Write data to the file
                yield

        self.log(f"Saved file {filename}")

    async def errback(self, failure):
        self.logger.error(repr(failure))
        if failure.check(TargetClosedError):
            self.logger.error("TargetClosedError occurred")

    def closed(self, reason):
        self.logger.info(f"Spider closed: {reason}")
        if hasattr(self, "playwright_browser"):
            self.playwright_browser.close()
