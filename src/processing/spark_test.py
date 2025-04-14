from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, stddev, min, max
import random

# Create a Spark session
spark = SparkSession.builder \
    .appName("RandomNumbersStatistics") \
    .master("local[*]") \
    .getOrCreate()

# Generate a list of 50 random numbers
data = [(random.random(),) for _ in range(50)]

# Create a DataFrame with a single column "value"
df = spark.createDataFrame(data, ["value"])
df.show()

# Use the describe() method to get count, mean, stddev, min, and max
print("Statistics using describe():")
df.describe().show()

# Alternatively, perform aggregations directly
print("Statistics using aggregations:")
stats_df = df.agg(
    mean("value").alias("mean"),
    stddev("value").alias("stddev"),
    min("value").alias("min"),
    max("value").alias("max")
)
stats_df.show()

# Stop the Spark session
spark.stop()