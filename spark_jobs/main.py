import sys

from pyspark.sql import SparkSession

from reader import read_csv
from validator import validate_data
from writer import save_dataframe

from config import DATASETS


if len(sys.argv) != 2:
    print("Usage:")
    print("spark-submit main.py equity")
    exit()


division = sys.argv[1]

dataset = DATASETS[division]

spark = SparkSession.builder \
    .appName(f"{division} DQ Pipeline") \
    .getOrCreate()


print(f"\nProcessing Division : {division}")


df = read_csv(
    spark,
    dataset["input"]
)


good_df, bad_df = validate_data(
    df,
    division
)


save_dataframe(
    good_df,
    dataset["good"]
)


save_dataframe(
    bad_df,
    dataset["bad"]
)


print("\nPipeline Completed Successfully!")

spark.stop()