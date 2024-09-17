from bs4 import BeautifulSoup

# Specify the file path
file_path = "/Users/hamish.macdonald/Documents/investing/scrapequity/sandbox/yahoo_poc/yahoo-finance-history-20240913144154-CBA.AX.html"

# Read the file content
with open(file_path, "r", encoding="utf-8") as file:
    file_content = file.read()

# Parse the HTML content
soup = BeautifulSoup(file_content, "html.parser")

# Find the table with the specific class
table = soup.find("table", {"data-test": "historical-prices"})

# Extract all rows from the table
rows = table.find_all("tr")

# Iterate over the rows and extract data
for row in rows:
    columns = row.find_all("td")
    if columns:  # Skip header rows
        data = [col.get_text(strip=True) for col in columns]
        print(data)
