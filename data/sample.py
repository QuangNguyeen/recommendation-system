import os
from pyspark.sql import SparkSession

# Setup Local
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_DRIVER_PORT"] = "7077"

# create SparkSession
spark = SparkSession \
    .builder \
    .appName("StratifiedSample") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# database & collection
database = "amazon_review_data"
collection = "user_reviews"
size = 100000

# Đọc dữ liệu từ MongoDB với Spark
sample = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", f"mongodb://localhost:27017/{database}.{collection}") \
    .option("partitioner", "MongoSinglePartitioner") \
    .option("partitionKey", "asin") \
    .option("pipeline", f'''[
        {{
            "$match": {{
                "asin": {{"$exists": true}}
            }}
        }},
        {{
            "$sample": {{
                "size": {size}
            }}
        }}
    ]''') \
    .load()


print(f"Stratified sample of {size} reviews extracted from the `{collection}` collection of the `{database}` dataset successfully.")

# Save CSV file
output_path = "/data/user_reviews.csv"
sample.toPandas().to_csv(output_path, index=False)

print(f"Stratified sample of {size} reviews saved as a CSV file successfully at {output_path}")

# Dừng SparkSession
spark.stop()
