from pyspark.sql.functions import col
import re

def camel_to_snake(name):
    """
    Convert a string from camelCase or PascalCase to snake_case.

    Args:
        name (str): The column name in camelCase or PascalCase.

    Returns:
        str: The converted string in snake_case.
    """
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()
def transform_title_ratings(df_ratings):
    """
    Transforms the title ratings DataFrame by applying:
    1. Column name normalization to snake_case.
    2. Data type corrections for rating and vote count columns.
    3. Missing value handling.

    Args:
        df_ratings (DataFrame): Original DataFrame to transform.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    df_ratings = df_ratings.select([col(column).alias(camel_to_snake(column)) for column in df_ratings.columns])
    df_ratings = df_ratings.withColumn("average_rating", col("average_rating").cast("float")) \
                           .withColumn("votes_number", col("votes_number").cast("int"))

    df_ratings = df_ratings.fillna({"average_rating": 0.0, "votes_number": 0})

    return df_ratings