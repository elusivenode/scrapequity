import scrapy, os, glob, math
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from scrapy_playwright.page import PageMethod
from playwright._impl._errors import TargetClosedError
from scrapy.selector import Selector
from datetime import datetime, timezone


class ScrollScrapeSpiderSpider(scrapy.Spider):

    name = "scroll_n_scrape"

    def __init__(self, param1=None, param2=None, *args, **kwargs):
        super(ScrollScrapeSpiderSpider, self).__init__(*args, **kwargs)
        self.runForTicker = param1
        self.runStartDate = param2

    def start_requests(self):

        directory = "output/scraped_prices"
        if not os.path.exists(directory):
            os.makedirs(directory)

        files = glob.glob(os.path.join(directory, "*"))
        for f in files:
            os.remove(f)

        asx300_file = self.get_latest_file("output/asx300")
        if self.runForTicker == "all":
            asx300 = self.parse_first_column(asx300_file)[1:]
        else:
            asx300 = [self.runForTicker]
        self.ticker_tidying(asx300)

        unix_epoch_start = int(
            datetime.strptime(f"{self.runStartDate}", "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        unix_epoch_end = int(
            datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )

        date_range = f"?period1={unix_epoch_start}&period2={unix_epoch_end}&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
        url_template = (
            "https://au.finance.yahoo.com/quote/{ticker}.AX/history{date_range}"
        )
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        # asx300 = asx300[0:16]
        for batch in range(0, len(asx300), 8):
            urls = []
            for ticker in asx300[batch : min(batch + 8, len(asx300))]:
                urls.append(url_template.format(ticker=ticker, date_range=date_range))

            self.logger.info(
                f"Processing stocks {batch + 1} to {min(batch + 8, len(asx300))} in the ASX300"
            )
            for url in urls:
                self.logger.info(f"Running: {url}")
            for url in urls:
                yield scrapy.Request(
                    url,
                    headers=headers,
                    meta=dict(
                        playwright=True,
                        playwright_include_page=True,
                        playwright_page_methods=[
                            PageMethod("wait_for_selector", "table")
                        ],
                    ),
                    callback=self.parse,
                    errback=self.errback,
                )

    async def parse(self, response):

        page = response.meta["playwright_page"]
        scroll_height = await page.evaluate("() => document.body.scrollHeight")

        try:
            last_position = await page.evaluate("window.scrollY")

            while True:
                await page.evaluate(f"window.scrollBy(0, {scroll_height})")
                await page.wait_for_timeout(100)
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
        directory = "output/scraped_prices"
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

        self.logger.info(f"Saved file {filename}")
        # Close the Playwright page
        self.logger.info(f"Closing: {await page.title()}")
        await page.close()

    async def errback(self, failure):
        self.logger.error(repr(failure))
        if failure.check(TargetClosedError):
            self.logger.error("TargetClosedError occurred")

    def closed(self, reason):
        self.logger.info(f"Spider closed: {reason}")
        if hasattr(self, "playwright_browser"):
            self.playwright_browser.close()

    def get_latest_file(self, directory):
        files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]

        # Sort files by modification time
        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        # Return the most recent file
        return files[0] if files else None

    def parse_first_column(self, file_path):
        first_column = []

        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                columns = line.strip().split("|")
                if columns:
                    first_column.append(columns[0])

        return first_column

    def change_list_value(self, my_list, index, new_value):
        if 0 <= index < len(my_list):
            my_list[index] = new_value
        else:
            self.logger.error("Index out of range")

    def find_index(self, my_list, value):
        try:
            return my_list.index(value)
        except ValueError:
            return None

    def ticker_tidying(self, asx300_l):
        # logs an error if the ticker is not found in the ASX300 list
        ticker = "ABP"
        idx = self.find_index(asx300_l, ticker)
        if idx is not None:
            self.change_list_value(asx300_l, idx, "ABG")
        else:
            self.logger.error(f"Couldn't find {ticker} in the ASX300 list")
