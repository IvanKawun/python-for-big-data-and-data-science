import pyspark
import pyspark.sql.types as t
from pyspark.sql import SparkSession


def basic_test_df():

    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("basic_dfs_moshkovskyi")
                                 .getOrCreate()
                     )

    schema = t.StructType([
        t.StructField("Name", t.StringType(), True),
        t.StructField("Age", t.IntegerType(), True),
        t.StructField("City", t.StringType(), True)
    ])

    data = [("Andriy", 24, "Kyiv"),("Oksana", 45, "Brovary"),("Karolina", 20, "Dnipro")]

    result = spark_session.createDataFrame(data, schema)

    return result