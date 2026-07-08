from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient

spark = SparkSession.builder \
    .appName("Bond Data Quality") \
    .getOrCreate()

df = spark.read.csv(
    "/opt/spark/data/raw/bond/bond_trades.csv",
    header=True,
    inferSchema=True
)

print("\n========== ORIGINAL DATA ==========")
df.show(truncate=False)

good_df = df.filter(
    (col("trade_id").isNotNull()) &
    (col("isin").isNotNull()) &
    (col("isin").rlike("^[A-Z]{2}[A-Z0-9]{10}$")) &
    (col("maturity_date").isNotNull()) &
    (col("face_value") > 0) &
    (col("yield_rate") > 0)
)

bad_df = df.subtract(good_df)

print("\n========== GOOD DATA ==========")
good_df.show(truncate=False)

print("\n========== BAD DATA ==========")
bad_df.show(truncate=False)

print(f"\nGood Records : {good_df.count()}")
print(f"Bad Records  : {bad_df.count()}")

client = MongoClient("mongodb://trading-mongodb:27017")

db = client["trading"]

good_collection = db["bond_good"]
bad_collection = db["bond_bad"]

good_records = []

for row in good_df.collect():
    record = {}

    for key, value in row.asDict().items():
        record[key] = None if value is None else str(value)

    good_records.append(record)

bad_records = []

for row in bad_df.collect():
    record = {}

    for key, value in row.asDict().items():
        record[key] = None if value is None else str(value)

    bad_records.append(record)

print("\nGood records ready :", len(good_records))
print("Bad records ready  :", len(bad_records))

if good_records:
    result = good_collection.insert_many(good_records)
    print(f"Inserted {len(result.inserted_ids)} GOOD records")

if bad_records:
    result = bad_collection.insert_many(bad_records)
    print(f"Inserted {len(result.inserted_ids)} BAD records")

client.close()

print("\n========== DATA STORED IN MONGODB ==========")

spark.stop()