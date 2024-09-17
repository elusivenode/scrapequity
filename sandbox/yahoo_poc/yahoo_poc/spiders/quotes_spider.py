import scrapy, os, glob
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup


class YahooSpider(scrapy.Spider):
    name = "yahoo_finance"

    directory = "output"
    if not os.path.exists(directory):
        os.makedirs(directory)

    files = glob.glob(os.path.join(directory, "*"))
    for f in files:
        os.remove(f)

    def start_requests(self):
        urls = [
            "https://au.finance.yahoo.com/quote/CBA.AX/history/",
            "https://au.finance.yahoo.com/quote/NAB.AX/history/",
            "https://au.finance.yahoo.com/quote/FBU.AX/history/",
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

        directory = "output"
        filename = os.path.join(
            directory, f"yahoo-finance-history-{timestamp}-{ticker}.html"
        )
        html = response.css("#Col1-1-HistoricalDataTable-Proxy").get()
        soup = BeautifulSoup(html, "html.parser")

        # Find the table with the specific class
        table = soup.find("table", {"data-test": "historical-prices"})

        # Extract all rows from the table
        rows = table.find_all("tr")

        # Open the file in append mode
        with open(filename, "a", encoding="utf-8") as file:
            # Extract the header row
            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all("th")]
            file.write("\t".join(headers) + "\n")  # Write headers to the file

            # Iterate over the rows and extract data
            for row in rows[1:]:  # Skip the header row
                columns = row.find_all("td")
                if columns:  # Skip empty rows
                    data = [col.get_text(strip=True) for col in columns]
                    file.write("\t".join(data) + "\n")  # Write data to the file

        self.log(f"Saved file {filename}")
