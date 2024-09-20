import scrapy, os, glob
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup


class ASX300Spider(scrapy.Spider):

    name = "asx300_scrape"

    def start_requests(self):

        directory = "output/asx300"
        if not os.path.exists(directory):
            os.makedirs(directory)

        files = glob.glob(os.path.join(directory, "*"))
        print(files)
        for f in files:
            os.remove(f)

        urls = [
            "https://www.asx300list.com/",
        ]
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        for url in urls:
            yield scrapy.Request(url=url, headers=headers, callback=self.parse)

    def parse(self, response):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        directory = "output/asx300"
        filename = os.path.join(
            directory, f"asx300-constituents-{timestamp}.txt"
        )
    
        table_class = "tableizer-table"
        table = response.css(f"table.{table_class}").get()
        soup = BeautifulSoup(table, "html.parser")
        rows = soup.find_all("tr")

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