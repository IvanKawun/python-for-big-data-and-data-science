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
from io1 import read_ratings_df, write_ratings_df_to_csv
from io1 import RATINGS_FILE_PATH
from io1 import RESULTS_PACKAGE_PATH
from title_ratings_editing import transform_title_ratings

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("ratings_dfs")
                 .getOrCreate()
                )

df_ratings = read_ratings_df(RATINGS_FILE_PATH, spark_session)
df_transformed = transform_title_ratings(df_ratings)
df_transformed.show()
write_ratings_df_to_csv(df_transformed, RESULTS_PACKAGE_PATH)

