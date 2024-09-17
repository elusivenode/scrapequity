import scrapy, os, glob
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup


class ASX300Spider(scrapy.Spider):
    name = "asx300_constituents"

    def start_requests(self):
        urls = [
            "https://www.asx300list.com/",
        ]
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        table_class = "tableizer-table"
        table = response.css(f"table.{table_class}")

        table_html = table.get()
        print(f"Table HTML:\n{table_html}")
