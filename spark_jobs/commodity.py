from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("Commodity Data Quality") \
    .getOrCreate()

# Read CSV
df = spark.read.csv(
    "/opt/spark/data/raw/commodity/commodity_contracts.csv",
    header=True,
    inferSchema=True
)

print("Original Data")
df.show()

# Good Records
good_df = df.filter(
    (col("contract_id").isNotNull()) &
    (col("product_code").isNotNull()) &
    (col("product_code") != "") &
    (col("expiry_date").isNotNull()) &
    (col("quantity") > 0) &
    (col("price") > 0)
)

# Bad Records
bad_df = df.subtract(good_df)

print("\nGood Records")
good_df.show()

print("\nBad Records")
bad_df.show()

# Save Output
good_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/good/commodity"
)

bad_df.write.mode("overwrite").option("header", True).csv(
    "/opt/spark/data/processed/bad/commodity"
)

spark.stop()