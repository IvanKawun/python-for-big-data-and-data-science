from pyspark.sql import DataFrame
from pyspark.sql.functions import trim


def movies_with_ratings(df_basics: DataFrame, df_ratings: DataFrame) -> DataFrame:
    """
    Finds movies that have a rating and the number of votes they received.

    :param df_basics: DataFrame from title.basics.tsv
    :param df_ratings: DataFrame from title.ratings.tsv
    :return: DataFrame of movies with ratings and number of votes
    """
    # Ensure the keys match in format
    df_basics = df_basics.withColumn('tconst', trim(df_basics['tconst']))
    df_ratings = df_ratings.withColumn('id', trim(df_ratings['id']))

    return df_basics.join(df_ratings, df_basics['tconst'] == df_ratings['id'], 'inner') \
        .select(df_basics['primary_title'], df_ratings['average_rating'], df_ratings['votes_number'])


def movies_with_known_principals(df_basics: DataFrame, df_principals: DataFrame) -> DataFrame:
    """
    Finds movies with known principals (cast or crew).

    :param df_basics: DataFrame from title.basics.tsv
    :param df_principals: DataFrame from title.principals.tsv
    :return: DataFrame of movies with known principals
    """
    # Ensure the keys match in format
    df_basics = df_basics.withColumn('tconst', trim(df_basics['tconst']))
    df_principals = df_principals.withColumn('tconst', trim(df_principals['tconst']))

    return df_basics.join(df_principals, df_basics['tconst'] == df_principals['tconst'], 'right') \
        .select(df_principals['tconst'], df_basics['primary_title'], df_principals['category'])