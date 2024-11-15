import re
from pyspark.sql import functions as F


def to_snake_case(camel_str):
    """Конвертуємо в snake_case"""
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', camel_str).lower()


def transform_title_basics(df):

    new_columns = [to_snake_case(col) for col in df.columns]
    df = df.toDF(*new_columns)

    for col in df.columns:
        df = df.withColumn(col, F.regexp_replace(F.col(col), r"^t", ""))

    if 'end_year' in df.columns:
        df = df.withColumn("end_year", F.when(F.col("end_year") == "\\N", None).otherwise(F.col("end_year")))
    elif 'endYear' in df.columns:
        df = df.withColumn("end_year", F.when(F.col("endYear") == "\\N", None).otherwise(F.col("endYear")))

    return df