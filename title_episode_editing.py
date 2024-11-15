import re
from pyspark.sql import functions as F

def to_snake_case(camel_str):
    """Converting to snake case"""
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', camel_str).lower()

def transform_title_episode(df_episode):
    df_episode = df_episode.withColumn("season_number",
                                       F.when(F.col("seasonNumber") == "\\N", None).otherwise(F.col("seasonNumber"))) \
                           .withColumn("episode_number",
                                       F.when(F.col("episodeNumber") == "\\N", None).otherwise(F.col("episodeNumber")))

    df_episode = df_episode.drop("seasonNumber", "episodeNumber")

    new_columns = [to_snake_case(col) for col in df_episode.columns]
    df_episode = df_episode.toDF(*new_columns)

    return df_episode