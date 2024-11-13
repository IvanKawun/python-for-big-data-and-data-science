from pyspark.sql import functions as F
import re


def to_snake_case(col_name):
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', col_name).lower()


def transform_title_akas(df_akas):
    new_columns = [to_snake_case(col) for col in df_akas.columns]
    df_akas = df_akas.toDF(*new_columns)

    for col in df_akas.columns:
        df_akas = df_akas.withColumn(col, F.when(F.col(col) == "\\N", None).otherwise(F.col(col)))
        
    df_akas = df_akas.withColumn("is_original_title", F.col("is_original_title").cast("boolean"))

    return df_akas