print(
    "################################################################################"
)
print("Use standard python libraries to do the transformations")
print(
    "################################################################################"
)

# Question: How do you read data from a CSV file at ./data/sample_data.csv into a list of dictionaries?
import csv

csv_file_location = "./data/sample_data.csv"
data = []
with open(csv_file_location, "r") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)
print(data)

# Question: How do you remove duplicate rows based on customer ID?
existing_ids = set()
duplicates_removed = []
for index, row in enumerate(data):
    id = row["Customer_ID"]
    if id not in existing_ids:
        duplicates_removed.append(row)
        existing_ids.add(id)
    else:
        print(f"Duplicated row found for id {id}")

# Question: How do you handle missing values by replacing them with 0?
for row in duplicates_removed:
    if not row["Age"]:
        row["Age"] = 0
    if not row["Purchase_Amount"]:
        row["Purchase_Amount"] = 0.0

# Question: How do you remove outliers such as age > 100 or purchase amount > 1000?
outliers_removed = [
    row
    for row in duplicates_removed
    if int(row["Age"]) <= 100 and float(row["Purchase_Amount"]) <= 1000
]

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male)?
for row in outliers_removed:
    match row["Gender"]:
        case "Male":
            row["Gender"] = 1
        case "Female":
            row["Gender"] = 0
        case _:
            print("Unexpected value for 'Gender' column")

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns?
for row in outliers_removed:
    row["First_Name"], row["Last_Name"] = row["Customer_Name"].split(" ", 1)
    del row["Customer_Name"]

# Question: How do you calculate the total purchase amount by Gender?
total_purchase_by_gender = {}
for row in outliers_removed:
    total_purchase_by_gender[row["Gender"]] = (
        total_purchase_by_gender.get(row["Gender"], 0.0)
        + float(row["Purchase_Amount"])
    )

# Question: How do you calculate the average purchase amount by Age group?
# assume age_groups is the grouping we want
# hint: Why do we convert to float?
age_groups = {"18-30": [], "31-40": [], "41-50": [], "51-60": [], "61-70": []}
for row in outliers_removed:
    age = int(row["Age"])
    for age_group, purchase_list in age_groups.items():
        min_age, max_age = (int(age) for age in age_group.split("-"))
        if min_age <= age <= max_age:
            purchase_list.append(float(row["Purchase_Amount"]))

average_purchase_by_age_group = {
    age_group: sum(purchase_list)/len(purchase_list) for age_group, purchase_list in age_groups.items()
}

print(f"Total purchase amount by Gender: {total_purchase_by_gender}")
print(f"Average purchase amount by Age group: {average_purchase_by_age_group}")

print(
    "################################################################################"
)
print("Use DuckDB to do the transformations")
print(
    "################################################################################"
)

# Question: How do you connect to DuckDB and load data from a CSV file into a DuckDB table?
# Connect to DuckDB and load data
import duckdb

duckdb_conn = duckdb.connect(database=":memory:", read_only=False)

# Read data from CSV file into DuckDB table
duckdb_conn.execute("""CREATE TABLE sample_data (
    Customer_ID INTEGER,
    Customer_Name VARCHAR,
    Age INTEGER,
    Gender VARCHAR,
    Purchase_Amount FLOAT,
    Purchase_Date DATE
)""")

duckdb_conn.execute(f"COPY sample_data FROM '{csv_file_location}' (AUTO_DETECT TRUE);")

# Question: How do you remove duplicate rows based on customer ID in DuckDB?
duplicates_removed_query = """
CREATE OR REPLACE TABLE duplicates_removed AS
SELECT
    *,
    ROW_NUMBER()
    OVER (
        PARTITION BY customer_id
    ) AS duplicate_number
FROM sample_data
ORDER BY customer_id;
DELETE FROM duplicates_removed
WHERE duplicate_number <> 1;
"""
duckdb_conn.sql(duplicates_removed_query)

# Question: How do you handle missing values by replacing them with 0 in DuckDB?
missing_values_removed_query = """
CREATE OR REPLACE TABLE missing_values_removed AS
SELECT
    customer_id,
    customer_name,
    COALESCE(age, 0) AS Age,
    gender,
    COALESCE(purchase_amount, 0.0) AS Purchase_Amount,
    purchase_date
FROM duplicates_removed
"""
duckdb_conn.execute(missing_values_removed_query)

# Question: How do you remove outliers (e.g., age > 100 or purchase amount > 1000) in DuckDB?
outliers_removed_query = """
CREATE OR REPLACE TABLE outliers_removed AS
SELECT *
FROM missing_values_removed
WHERE age <= 100 AND purchase_amount <= 1000;
"""
duckdb_conn.execute(outliers_removed_query)

# Question: How do you convert the Gender column to a binary format (0 for Female, 1 for Male) in DuckDB?
gender_converted_query = """
CREATE OR REPLACE TABLE gender_converted AS
SELECT customer_id, customer_name, age,
    CASE
        WHEN gender = 'Male' THEN 1
        WHEN gender = 'Female' THEN 0
    END AS gender, purchase_amount, purchase_date
FROM outliers_removed;
"""
duckdb_conn.execute(gender_converted_query)

# Question: How do you split the Customer_Name column into separate First_Name and Last_Name columns in DuckDB?
name_split_query = """
CREATE OR REPLACE TABLE name_split AS
SELECT
    customer_id,
    SPLIT_PART(customer_name, ' ', 1) AS first_name,
    SPLIT_PART(customer_name, ' ', 2) AS last_name,
    age,
    gender,
    purchase_amount,
    purchase_date
FROM outliers_removed;
"""
duckdb_conn.execute(name_split_query)

# Question: How do you calculate the total purchase amount by Gender in DuckDB?
total_purchase_by_gender_query = """
CREATE OR REPLACE TABLE total_purchase_by_gender AS
SELECT
    gender,
    SUM(purchase_amount) AS total_purchase
FROM name_split
GROUP BY gender;
"""
duckdb_conn.execute(total_purchase_by_gender_query)

# Question: How do you calculate the average purchase amount by Age group in DuckDB?
average_purchase_by_age_group_query = """
CREATE OR REPLACE TABLE average_purchase_by_age_group AS
SELECT
    CASE
        WHEN 18 <= age and age <= 30 THEN '18-30'
        WHEN age <= 40 THEN '31-40'
        WHEN age <= 50 THEN '41-50'
        WHEN age <= 60 THEN '51-60'
        WHEN age <= 70 THEN '61-70'
        ELSE 'Unexpected'
    END AS age_group,
    AVG(purchase_amount) AS average_purchase
FROM name_split
GROUP BY 1
ORDER BY 1;
"""
duckdb_conn.execute(average_purchase_by_age_group_query)

# Question: How do you print the results for total purchase amount by Gender and average purchase amount by Age group in DuckDB?
print("====================== Results ======================")
print("Total purchase amount by Gender:")
duckdb_conn.sql("SELECT * FROM total_purchase_by_gender;")
print("Average purchase amount by Age group:")
duckdb_conn.sql("SELECT * FROM average_purchase_by_age_group;")

duckdb_conn.close()
