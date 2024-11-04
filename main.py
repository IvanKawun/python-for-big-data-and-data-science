# Не використовувати Pandas та UDF
# Без collect() та toPandas()
"""
from basic_dfs.basic_dfs_moshkovskyi import basic_test_df

def view_dataframe():
    df = basic_test_df()
    df.show()

view_dataframe()
"""

import pyspark.sql.types as t
from pyspark.sql import SparkSession
from io1 import read_ratings_df
from io1 import RATINGS_FILE_PATH

spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("ratings_dfs")
                                 .getOrCreate()
                     )
df = read_ratings_df(RATINGS_FILE_PATH, spark_session)
