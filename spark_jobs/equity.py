from pyspark.sql import SparkSession
from pymongo import MongoClient

from validation_engine import (
    get_validation_rules,
    apply_validation_rules
)


# ==========================================
# Dataset
# ==========================================
DATASET = "equity"


# ==========================================
# Spark Session
# ==========================================
spark = SparkSession.builder \
    .appName("Equity Data Quality") \
    .getOrCreate()


# ==========================================
# Read CSV
# ==========================================
df = spark.read.csv(
    "/opt/spark/data/raw/equity/equity_trades.csv",
    header=True,
    inferSchema=True
)


print("\n========== ORIGINAL DATA ==========")

df.show(truncate=False)


# ==========================================
# Get Rules
# ==========================================
rules = get_validation_rules(
    DATASET
)


print("\n========== VALIDATION RULES ==========")

for rule in rules:
    print(rule)


# ==========================================
# Apply Rules
# ==========================================
good_df, bad_df = apply_validation_rules(
    df,
    rules
)


print("\n========== GOOD DATA ==========")

good_df.show(
    truncate=False
)


print("\n========== BAD DATA ==========")

bad_df.show(
    truncate=False
)


good_count = good_df.count()

bad_count = bad_df.count()

total_count = good_count + bad_count


print(
    f"\nGood Records : {good_count}"
)

print(
    f"Bad Records  : {bad_count}"
)


# ==========================================
# MongoDB Connection
# ==========================================
client = MongoClient(
    "mongodb://trading-mongodb:27017"
)

db = client["trading"]

good_collection = db[
    "equity_good"
]

bad_collection = db[
    "equity_bad"
]


# ==========================================
# Convert Spark Rows
# ==========================================
def convert_records(dataframe):

    records = []

    for row in dataframe.collect():

        record = {}

        for key, value in row.asDict().items():

            if value is None:

                record[key] = None

            elif isinstance(value, list):

                record[key] = [
                    str(item)
                    for item in value
                ]

            else:

                record[key] = str(value)

        records.append(record)

    return records


good_records = convert_records(
    good_df
)

bad_records = convert_records(
    bad_df
)


# ==========================================
# Clear Previous Results
# ==========================================
good_collection.delete_many({})

bad_collection.delete_many({})


# ==========================================
# Insert Good Records
# ==========================================
if good_records:

    result = good_collection.insert_many(
        good_records
    )

    print(
        f"Inserted "
        f"{len(result.inserted_ids)} "
        f"GOOD records"
    )


# ==========================================
# Insert Bad Records
# ==========================================
if bad_records:

    result = bad_collection.insert_many(
        bad_records
    )

    print(
        f"Inserted "
        f"{len(result.inserted_ids)} "
        f"BAD records"
    )


client.close()


print(
    "\n========== DATA STORED IN MONGODB =========="
)


spark.stop()