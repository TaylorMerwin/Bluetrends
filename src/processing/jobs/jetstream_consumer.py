from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JetstreamConsumer") \
    .master("spark://spark:7077") \
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
    StructField("reply_to", MapType(StringType(), StringType()), True)
])

# Parse the JSON string column into a structured DataFrame
df_parsed = df_string.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Write the stream to the console, for testing
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()