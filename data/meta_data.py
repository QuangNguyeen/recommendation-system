import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, MapType

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
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("main_category", StringType(), False),  #Main category (i.e., domain) of the product.
    StructField("title", StringType(), False),          # Name of product
    StructField("average_rating", DoubleType(), False), # Rating of the product shown on the product page
    StructField("rating_number", IntegerType(), False), # Number of rating in the product
    StructField("features", MapType(StringType(), StringType()), False),    # Bullet-point format features of the product
    StructField("description", MapType(StringType(), StringType()), False), # Description of the product
    StructField("price", DoubleType(), False),                              # Price in US Dollar
    StructField("images", MapType(StringType(), StringType()), False),      # Images of the product
    StructField("videos", MapType(StringType(), StringType()), False),      # Videos of the product including video and url
    StructField("store", StringType(), False),                              # Store name of the product
    StructField("categories", MapType(StringType(), StringType()), False),  # Hierarchical categories of product
    StructField("details", MapType(StringType(), StringType()), False),     # Product details, including materials, brand, size, etc
    StructField("parent_asin", StringType(), False),                        # Parent ID of product
    StructField("bought_together", StringType(), False)                     # Recommended bundles from websites
])

# Read dataset from JSON file into Spark DataFrame
dataframe = spark.read.format("json").option("mode", "DROP MALFORMED").schema(schema).load("meta_All_Beauty.jsonl.gz")
print("JavaScript Object Notation (JSON) file stored in Spark DataFrame successfully")

# Writing th Spark DataFrame into MongoDB database as a collection
database = "amazon_review_data"
collection = "meta_all_beauty"
dataframe.select("main_category","title" ,"average_rating", "rating_number", "features", "description", "price",
                 "images", "videos", "store", "categories", "details", "parent_asin", "bought_together").write \
         .format("com.mongodb.spark.sql.DefaultSource") \
         .option("uri", "mongodb://localhost:27017/" + database + "." + collection) \
         .option("replaceDocument", "false") \
         .option("partitioner", "MongoSinglePartitioner") \
         .option("partitionKey", "parent_asin") \
         .save()



print("Spark DataFrame stored in the `"+database+"` database as the `"+collection+"` collection successfully!")
spark.stop()    #   Stopping the SparkSession object.

