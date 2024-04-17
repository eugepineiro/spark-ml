from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = (SparkSession.builder
         .appName("Spark ML")
         .config("spark.memory.offHeap.enabled","true") # the data was cached in off-heap memory to avoid storing it directly on disk
         .config("spark.memory.offHeap.size","10g")
         .getOrCreate())


spark_df = spark.read.csv('data/patients.csv', header=True, escape="\"")

spark_df.printSchema()

print(spark_df.columns)
spark_df.select("age").describe().show()