from pyspark.sql import SparkSession

def read_csv(spark, file_path):

    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    return df