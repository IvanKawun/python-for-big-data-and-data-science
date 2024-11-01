from pyspark.sql import SparkSession
# Не використовувати Pandas та UDF
# Без collect() та toPandas()
spark = SparkSession.builder \
    .appName("My Spark Application") \
    .getOrCreate()

print("This is the project of Moshkovskyi Ivan")
print("Spark session is created:", spark)

spark.stop()