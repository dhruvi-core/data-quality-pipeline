from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymongo import MongoClient

# ==========================================
# Create Spark Session
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
# Good Records
# ==========================================
good_df = df.filter(
    (col("trade_id").isNotNull()) &
    (col("instrument").isNotNull()) &
    (col("instrument") != "") &
    (col("quantity") > 0) &
    (col("price") > 0) &
    (col("trade_date").isNotNull())
)

# ==========================================
# Bad Records
# ==========================================
bad_df = df.subtract(good_df)

print("\n========== GOOD DATA ==========")
good_df.show(truncate=False)

print("\n========== BAD DATA ==========")
bad_df.show(truncate=False)

print(f"\nGood Records : {good_df.count()}")
print(f"Bad Records  : {bad_df.count()}")

# ==========================================
# Connect MongoDB
# ==========================================
client = MongoClient("mongodb://trading-mongodb:27017")

db = client["trading"]

good_collection = db["equity_good"]
bad_collection = db["equity_bad"]

# ==========================================
# Convert Spark DataFrame -> Python Dictionary
# ==========================================
good_records = []

for row in good_df.collect():
    record = {}

    for key, value in row.asDict().items():
        if value is None:
            record[key] = None
        else:
            record[key] = str(value)

    good_records.append(record)

bad_records = []

for row in bad_df.collect():
    record = {}

    for key, value in row.asDict().items():
        if value is None:
            record[key] = None
        else:
            record[key] = str(value)

    bad_records.append(record)

print("\nGood records ready for MongoDB :", len(good_records))
print("Bad records ready for MongoDB  :", len(bad_records))

# ==========================================
# Insert Good Data
# ==========================================
if len(good_records) > 0:
    result = good_collection.insert_many(good_records)
    print(f"Inserted {len(result.inserted_ids)} GOOD records")

# ==========================================
# Insert Bad Data
# ==========================================
if len(bad_records) > 0:
    result = bad_collection.insert_many(bad_records)
    print(f"Inserted {len(result.inserted_ids)} BAD records")

client.close()

print("\n========== DATA STORED IN MONGODB ==========")

spark.stop()