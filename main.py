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
from pyspark.sql.pandas.types import from_arrow_type

from io1 import read_ratings_df, write_ratings_df_to_csv
from io1 import RATINGS_FILE_PATH
from io1 import RESULTS_PACKAGE_PATH
from title_ratings_editing import transform_title_ratings
from title_crew_editing import transform_title_crew
from title_principals_editing import transform_title_principals
from name_basics_editing import transform_name_basics
from title_akas_editing import transform_title_akas

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("ratings_dfs")
                 .getOrCreate()
                )
"""
df_ratings = read_ratings_df(RATINGS_FILE_PATH, spark_session)
df_transformed = transform_title_ratings(df_ratings)
write_ratings_df_to_csv(df_transformed, RESULTS_PACKAGE_PATH)

df_crew = spark_session.read.option("delimiter", "\t").csv("data/title.crew.tsv.gz", header=True)
df_crew_transformed = transform_title_crew(df_crew)
write_ratings_df_to_csv(df_crew_transformed, RESULTS_PACKAGE_PATH)
"""
"""
df_principals = spark_session.read.option("delimiter", "\t").csv("data/title.principals.tsv.gz", header=True)
df_principals_transformed = transform_title_principals(df_principals)
write_ratings_df_to_csv(df_principals_transformed, RESULTS_PACKAGE_PATH)
"""
"""
df_basics = spark_session.read.option("delimiter", "\t").csv("data/name.basics.tsv.gz", header=True)
df_basics_transformed = transform_name_basics(df_basics)
write_ratings_df_to_csv(df_basics_transformed, RESULTS_PACKAGE_PATH)
"""

df_akas = spark_session.read.option("delimiter", "\t").csv("data/title.akas.tsv.gz", header=True)
df_akas_transformed = transform_title_akas(df_akas)
write_ratings_df_to_csv(df_akas_transformed, RESULTS_PACKAGE_PATH)
