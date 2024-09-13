from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

import scrapy


class YahooSpider(scrapy.Spider):
    name = "yahoo_finance"

    def start_requests(self):
        urls = [
            "https://au.finance.yahoo.com/quote/CBA.AX/history/",
        ]
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        page_details = response.url.split("/")
        idx = page_details.index("quote")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        ticker = page_details[idx + 1]
        filename = f"yahoo-finance-history-{timestamp}-{ticker}.html"
        html = response.css("#Col1-1-HistoricalDataTable-Proxy").get()
        soup = BeautifulSoup(html, "html.parser")
        pretty_html = soup.prettify()
        Path(filename).write_text(pretty_html)
        self.log(f"Saved file {filename}")
