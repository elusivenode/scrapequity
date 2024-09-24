import os, re
import psycopg2
from psycopg2 import sql

# Database connection parameters
DB_NAME = "scrapedb"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = "55000"

# Directory containing the .txt files
directory = "/Users/hamish.macdonald/Documents/investing/scrapequity/poc/prices_with_scroll_scraper/output/scraped_prices/bkup/scraped_20191231_20240920"

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()


def create_stage_tables():
    drop_table_query = "DROP TABLE IF EXISTS staging.scraped_data_price_volume;"
    create_table_query = """
    CREATE TABLE staging.scraped_data_price_volume (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        adj_close FLOAT,
        volume BIGINT
    );
    """
    cur.execute(drop_table_query)
    cur.execute(create_table_query)
    conn.commit()

    drop_table_query = "DROP TABLE IF EXISTS staging.scraped_data_dividend;"
    create_table_query = """
    CREATE TABLE staging.scraped_data_dividend (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10),
        date DATE,
        dividend FLOAT
    );
    """
    cur.execute(drop_table_query)
    cur.execute(create_table_query)
    conn.commit()


# Function to insert data into the table
def insert_data(target, data):
    if target == "scraped_data_price_volume":
        insert_query = """
        INSERT INTO staging.scraped_data_price_volume (ticker, date, open, high, low, close, adj_close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """

    elif target == "scraped_data_dividend":
        insert_query = """
        INSERT INTO staging.scraped_data_dividend (ticker, date, dividend)
        VALUES (%s, %s, %s);
        """

    cur.execute(insert_query, data)
    conn.commit()


create_stage_tables()

ct = 1
for filename in os.listdir(directory):
    if filename.endswith(".txt"):
        file_path = os.path.join(directory, filename)
        ticker = filename.split("-")[4].split(".")[0]
        print(f"Processing file {ct} which is ticker {ticker}:\n{file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            # Skip the header line
            next(file)
            for line in file:
                # Split the line into columns
                columns = line.strip().split("|")
                if len(columns) == 7 and columns[1] != "-":
                    # Convert data types as needed
                    ticker_ = ticker
                    date = columns[0]
                    open_price = float(columns[1])
                    high_price = float(columns[2])
                    low_price = float(columns[3])
                    close_price = float(columns[4])
                    adj_close = float(columns[5])
                    volume_str = columns[6].replace(",", "")
                    volume = None if volume_str == "-" else int(volume_str)
                    # Insert data into the database
                    insert_data(
                        "scraped_data_price_volume",
                        (
                            ticker_,
                            date,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            adj_close,
                            volume,
                        ),
                    )
                if len(columns) == 2 and "Dividend" in columns[1]:
                    # Convert data types as needed
                    ticker_ = ticker
                    date = columns[0]
                    dividend = float(re.match(r"(\d+(\.\d+)?)", columns[1]).group(1))
                    # Insert data into the database
                    insert_data(
                        "scraped_data_dividend",
                        (ticker_, date, dividend),
                    )
    ct += 1

cur.execute("CALL conformed.sp_merge_into_price_volume_scraped()")
conn.commit()

cur.close()
conn.close()
