import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, MapType, \
    LongType

# setup local environment
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_DRIVER_PORT"] = "7077"

# creating an instance of SparkSession object and connecting it to MongoDB

spark=SparkSession \
    .builder \
    .appName("Amazon Review Data") \
    .master("local") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("asin", StringType(), False),
    StructField("parent_asin", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("rating", DoubleType(), False),
    StructField("title", StringType(), False),
    StructField("text", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("verified_purchase", BooleanType(), False),
    StructField("helpful_vote", IntegerType(), False),
    StructField("images", MapType(StringType(), StringType()), False)
])

# read dataset from JSON file into Spark DataFrame

dataframe = spark.read.format("json").option("mode", "DROP MALFORMED").schema(schema).load("All_Beauty.jsonl.gz")
print("JavaScript Object Notation (JSON) file stored in Spark DataFrame successfully")

# Writing th Spark DataFrame into MongoDB database as a collection

database = "amazon_review_data"
collection = "user_reviews"
dataframe.select("asin","parent_asin" ,"user_id", "rating", "title", "text", "timestamp", "verified_purchase", "helpful_vote", "images").write \
         .format("com.mongodb.spark.sql.DefaultSource") \
         .option("uri", "mongodb://localhost:27017/" + database + "." + collection) \
         .option("replaceDocument", "false") \
         .option("partitioner", "MongoSinglePartitioner") \
         .option("partitionKey", "asin") \
         .save()



print("Spark DataFrame stored in the `"+database+"` database as the `"+collection+"` collection successfully!")
spark.stop()    #   Stopping the SparkSession object.

