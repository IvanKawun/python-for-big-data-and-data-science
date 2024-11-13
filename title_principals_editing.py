from pyspark.sql import functions as F
from pyspark.sql import types as t

def transform_title_principals(df_principals):
    df_principals = df_principals.withColumn("job", F.when(F.col("job") == "\\N", None).otherwise(F.col("job"))) \
                                 .withColumn("characters", F.when(F.col("characters") == "\\N", None).otherwise(F.col("characters")))

    df_principals = df_principals.withColumn("tconst", F.col("tconst").cast("string")) \
                                 .withColumn("ordering", F.col("ordering").cast("int")) \
                                 .withColumn("nconst", F.col("nconst").cast("string")) \
                                 .withColumn("category", F.col("category").cast("string")) \
                                 .withColumn("job", F.col("job").cast("string"))

    df_principals = df_principals.withColumn("characters", F.regexp_replace(F.col("characters"), r'[\[\]"]', ''))
    return df_principals