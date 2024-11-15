from pyspark.sql import functions as F


def transform_title_crew(df_crew):
    # Transforms the title_crew DataFrame by applying:
    # 1. Replacing '\\N' with null values in the 'writers' column.
    # 2. Casting columns to appropriate types.

    df_crew = df_crew.withColumn("writers", F.when(F.col("writers") == "\\N", None).otherwise(F.col("writers")))

    df_crew = df_crew.withColumn("tconst", F.col("tconst").cast("string")) \
        .withColumn("directors", F.col("directors").cast("string")) \
        .withColumn("writers", F.col("writers").cast("string"))

    return df_crew