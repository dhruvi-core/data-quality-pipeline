from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("Equity Data Quality") \
    .getOrCreate()

# Read CSV
df = spark.read.csv(
    "/opt/spark/data/raw/equity/equity_trades.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show()

# Good Records
good_df = df.filter(
    (col("trade_id").isNotNull()) &
    (col("instrument").isNotNull()) &
    (col("instrument") != "") &
    (col("quantity") > 0) &
    (col("price") > 0) &
    (col("trade_date").isNotNull())
)

# Bad Records
bad_df = df.subtract(good_df)

print("\nGood Records")
good_df.show()

print("\nBad Records")
bad_df.show()

# Save Output
good_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/good/equity"
)

bad_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/bad/equity"
)

spark.stop()