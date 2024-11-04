from setting import RATINGS_FILE_PATH
import pyspark.sql.types as t

def read_ratings_df(path, spark_session):
    df_schema = t.StructType([
        t.StructField("ID", t.StringType(), False),
        t.StructField("AverageRating", t.DoubleType(), True),
        t.StructField("VotesNumber", t.IntegerType(), True)
    ])

    read_df = spark_session.read.csv(path, header=True)
    return read_df