from pyspark.sql import SparkSession
from pymongo import MongoClient

from validation_engine import (
    get_validation_rules,
    apply_validation_rules
)


DATASET = "bond"


spark = SparkSession.builder \
    .appName("Bond Data Quality") \
    .getOrCreate()


# Read Bond CSV
df = spark.read.csv(
    "/opt/spark/data/raw/bond/bond_trades.csv",
    header=True,
    inferSchema=True
)


print("\n========== ORIGINAL BOND DATA ==========")
df.show(truncate=False)


# Get dynamic validation rules from PostgreSQL
rules = get_validation_rules(DATASET)


print("\n========== BOND VALIDATION RULES ==========")

for rule in rules:
    print(rule)


# Apply validation rules
good_df, bad_df = apply_validation_rules(
    df,
    rules
)


print("\n========== GOOD BOND DATA ==========")
good_df.show(truncate=False)


print("\n========== BAD BOND DATA ==========")
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

good_collection = db["bond_good"]
bad_collection = db["bond_bad"]


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


good_records = convert_records(good_df)
bad_records = convert_records(bad_df)


# Remove results from previous pipeline run
good_collection.delete_many({})
bad_collection.delete_many({})


# Store good records
if good_records:

    result = good_collection.insert_many(
        good_records
    )

    print(
        f"Inserted {len(result.inserted_ids)} "
        f"GOOD Bond records"
    )


# Store bad records with failure reasons
if bad_records:

    result = bad_collection.insert_many(
        bad_records
    )

    print(
        f"Inserted {len(result.inserted_ids)} "
        f"BAD Bond records"
    )


client.close()


print(
    "\n========== BOND DATA STORED IN MONGODB =========="
)


spark.stop()