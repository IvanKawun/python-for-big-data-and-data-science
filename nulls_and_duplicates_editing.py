from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col


def drop_writers_column(df_before):
    """
    Drops the 'writers' column from the DataFrame if it exists.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame without the 'writers' column.
    """
    if "writers" in df_before.columns:
        return df_before.drop("writers")
    return df_before
def drop_characters_column(df_before):
    """
    Drops the 'characters' column from the DataFrame if it exists.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame without the 'characters' column.
    """
    if "characters" in df_before.columns:
        return df_before.drop("characters")
    return df_before
def drop_job_column(df_before):
    """
    Drops the 'job' column from the DataFrame if it exists.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame without the 'job' column.
    """
    if "job" in df_before.columns:
        return df_before.drop("job")
    return df_before
def drop_language_column(df_before):
    """
    Drops the 'language' column from the DataFrame if it exists.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame without the 'language' column.
    """
    if "language" in df_before.columns:
        return df_before.drop("language")
    return df_before
def fill_null_start_year(df):
    """
    Replaces null values in the 'start_year' column with 0.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: DataFrame with null values in 'start_year' column replaced by 0.
    """
    return df.withColumn("start_year", when(col("start_year").isNull(), 0).otherwise(col("start_year")))
def nulls_filling(df: DataFrame) -> DataFrame:
    """
    Checks for null values in the DataFrame and fills them with 0.

    Args:
        df (DataFrame): DataFrame to check and clean.

    Returns:
        DataFrame: Cleaned DataFrame with null values filled with 0.
    """
    for column in df.columns:
        null_counter = df.filter(col(column).isNull()).count()
        if null_counter > 0:
            print(f"Column '{column}' has {null_counter} null values.")

    df = df.fillna(0)

    return df
def title_duplicates_removal(df):
    edited_df = df.dropDuplicates()

    original_count = df.count()
    cleaned_count = edited_df.count()
    duplicates_removed = original_count - cleaned_count

    print(f"Removed {duplicates_removed} duplicate rows.")
    return edited_df
def ratings_duplicates_removal(df: DataFrame) -> DataFrame:
    df_no_duplicates = df.dropDuplicates()

    original_count = df.count()
    cleaned_count = df_no_duplicates.count()
    duplicates_removed = original_count - cleaned_count

    print(f"Removed {duplicates_removed} duplicate rows.")

    return df_no_duplicates