from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("Bond Data Quality") \
    .getOrCreate()

# Read CSV
df = spark.read.csv(
    "/opt/spark/data/raw/bond/bond_positions.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show()

# Good Records
good_df = df.filter(
    (col("trade_id").isNotNull()) &
    (col("isin").isNotNull()) &
    (col("isin") != "INVALID") &
    (col("maturity_date").isNotNull()) &
    (col("face_value") > 0) &
    (col("yield_rate") > 0)
)

# Bad Records
bad_df = df.subtract(good_df)

print("\nGood Records")
good_df.show()

print("\nBad Records")
bad_df.show()

# Save Output
good_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/good/bond"
)

bad_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/bad/bond"
)

spark.stop()
