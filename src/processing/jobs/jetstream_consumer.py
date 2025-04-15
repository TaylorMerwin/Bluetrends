# src/processing/jobs/jetstream_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, MapType, NumericType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JetstreamConsumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "text_posts") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert binary value column to string
df_string = df.selectExpr("CAST(value AS STRING) as json_str")

# Define the schema matching our JSON structure
schema = StructType([
    StructField("did", StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("text", StringType(), True),
])

# Parse the JSON string column into a structured DataFrame
df_parsed = df_string.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Convert the 'createdAt' field to a timestamp
df_parsed = df_parsed.withColumn(
    "createdAt_ts",
    to_timestamp(col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
)

# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()