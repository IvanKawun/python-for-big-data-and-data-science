import re
from pyspark.sql import functions as F

def to_snake_case(camel_str):
    """Converting to snake case"""
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', camel_str).lower()

def transform_name_basics(df_basics):
    df_basics = df_basics.withColumn("birthYear", F.when(F.col("birthYear") == "\\N", None).otherwise(F.col("birthYear"))) \
                         .withColumn("deathYear", F.when(F.col("deathYear") == "\\N", None).otherwise(F.col("deathYear")))

    df_basics = df_basics.withColumn("nconst", F.col("nconst").cast("string")) \
                         .withColumn("primaryName", F.col("primaryName").cast("string")) \
                         .withColumn("birthYear", F.col("birthYear").cast("int")) \
                         .withColumn("deathYear", F.col("deathYear").cast("int")) \
                         .withColumn("primaryProfession", F.col("primaryProfession").cast("string")) \
                         .withColumn("knownForTitles", F.col("knownForTitles").cast("string"))

    new_columns = [to_snake_case(col) for col in df_basics.columns]
    df_basics = df_basics.toDF(*new_columns)

    return df_basics