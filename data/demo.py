from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("test").getOrCreate()
spark.range(10).show()
