from pyspark.sql import SparkSession
from pymongo import MongoClient

from validation_engine import (
    get_validation_rules,
    apply_validation_rules
)


DATASET = "commodity"


spark = SparkSession.builder \
    .appName("Commodity Data Quality") \
    .getOrCreate()


# Read Commodity CSV
df = spark.read.csv(
    "/opt/spark/data/raw/commodity/commodity_trades.csv",
    header=True,
    inferSchema=True
)


print("\n========== ORIGINAL COMMODITY DATA ==========")
df.show(truncate=False)


# Get dynamic validation rules from PostgreSQL
rules = get_validation_rules(DATASET)


print(
    "\n========== COMMODITY VALIDATION RULES =========="
)

for rule in rules:
    print(rule)


# Apply validation rules
good_df, bad_df = apply_validation_rules(
    df,
    rules
)


print("\n========== GOOD COMMODITY DATA ==========")
good_df.show(truncate=False)


print("\n========== BAD COMMODITY DATA ==========")
bad_df.show(truncate=False)


# Count records
good_count = good_df.count()
bad_count = bad_df.count()
total_count = good_count + bad_count


print(f"\nTotal Records : {total_count}")
print(f"Good Records  : {good_count}")
print(f"Bad Records   : {bad_count}")


# MongoDB connection
client = MongoClient(
    "mongodb://trading-mongodb:27017"
)

db = client["trading"]

good_collection = db[
    "commodity_good"
]

bad_collection = db[
    "commodity_bad"
]


# Convert Spark DataFrame rows to MongoDB records
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


# Remove previous pipeline results
good_collection.delete_many({})
bad_collection.delete_many({})


# Store good records
if good_records:

    result = good_collection.insert_many(
        good_records
    )

    print(
        f"Inserted {len(result.inserted_ids)} "
        f"GOOD Commodity records"
    )


# Store bad records with failure reasons
if bad_records:

    result = bad_collection.insert_many(
        bad_records
    )

    print(
        f"Inserted {len(result.inserted_ids)} "
        f"BAD Commodity records"
    )


client.close()


print(
    "\n========== COMMODITY DATA STORED IN MONGODB =========="
)


spark.stop()