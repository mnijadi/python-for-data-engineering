# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases

# Fetch data from the SQLite Customer table
import sqlite3

query = "SELECT * FROM customer"

with sqlite3.connect("tpch.db") as sqlite_conn:
    records = sqlite_conn.execute(query).fetchall()

# Insert data into the DuckDB Customer table
import duckdb

with duckdb.connect("duckdb.db") as duckdb_conn:
    duckdb_conn.executemany("INSERT INTO customer VALUES (?, ?, ?, ?, ?, ?)", records)
    duckdb_conn.commit()

# Hint: Look for Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted

# We should close the connection, as DB connections are expensive

# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8

# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access
import csv
import gzip
from io import StringIO

import boto3
from botocore import UNSIGNED
from botocore.config import Config


s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Download the CSV file from S3
csv_gz_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
compressed_csv = csv_gz_obj["Body"].read()

# Decompress the gzip data
csv_content = gzip.decompress(compressed_csv).decode("utf-8")

# Read the CSV file using csv.reader
data = list(csv.reader(StringIO(csv_content)))

# Connect to the DuckDB database (assume WeatherData table exists)
with duckdb.connect("duckdb.db") as duckdb_conn:
    duckdb_conn.executemany("INSERT INTO weatherdata VALUES (?, ?, ?, ?, ?, ?, ?, ?)", data)
    duckdb_conn.commit()

# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
response = requests.get(url)
print(response.status_code)
data = response.json()["data"]

# Connect to the DuckDB database
with duckdb.connect("duckdb.db") as duckdb_conn:
    duckdb_conn.executemany("""
    INSERT INTO exchanges VALUES (
        $exchangeId,
        $name,
        $rank,
        $percentTotalVolume,
        $volumeUsd,
        $tradingPairs,
        $socket,
        $exchangeUrl,
        $updated
    )""", data)
    duckdb_conn.commit()


# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python
csv_file_location = "./data/sample_data.csv"
with open(csv_file_location, "r") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    for record in csv_reader:
        print(", ".join(record))

# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'

import bs4

response = requests.get(url)
soup = bs4.BeautifulSoup(response.content, "html.parser")

all_links = [a["href"] for a in soup.find_all("a")]
print(all_links)