from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, col, count, size, split, desc, countDistinct
from pyspark.sql import DataFrame


def most_active_director(df: DataFrame) -> DataFrame:
    """
    Finds the director(s) who directed the most movies.

    :param df: DataFrame from title.crew.tsv
    :return: DataFrame with director(s) and their movie count
    """
    return df.withColumn('director', split(df['directors'], ',')[0]) \
        .groupBy('director') \
        .agg(count('*').alias('movie_count')) \
        .orderBy(desc('movie_count'))


def movies_with_three_genres(df: DataFrame) -> int:
    """
    Counts how many movies have exactly three genres.

    :param df: DataFrame from title.basics.tsv
    :return: Number of movies with three genres
    """
    return df.withColumn('genre_count', size(split(col('genres'), ','))) \
        .filter(col('genre_count') == 3) \
        .count()


def count_program_types(df: DataFrame) -> int:
    """
    Counts the number of distinct program types (e.g., movie, tvSeries).

    :param df: DataFrame from title.basics.tsv
    :return: Number of distinct program types
    """
    return df.select(countDistinct('title_type')).first()[0]


"Window functions"
def longest_tv_show(df: DataFrame) -> DataFrame:
    """
       Finds the longest-running TV show based on runtimeMinutes.

       :param df: DataFrame from title.basics.tsv
       :return: DataFrame with the longest-running TV show
       """
    window_spec = Window.orderBy(col('runtime_minutes').desc())

    # Filter for TV shows and non-null runtimeMinutes
    filtered_df = df.filter(
        (col('title_type').isin('tvSeries', 'tvMiniSeries')) &
        (col('runtime_minutes').isNotNull())
    )

    # Apply row_number to get the top TV show
    return filtered_df.withColumn('rank', row_number().over(window_spec)) \
        .filter(col('rank') == 1) \
        .drop('rank')

def earliest_born_person(df: DataFrame) -> DataFrame:
    """
    Finds the earliest born person with non-null birth year.

    :param df: DataFrame from name.basics.tsv
    :return: DataFrame with the earliest born person
    """
    window_spec = Window.orderBy(col('birthYear').cast('int').asc())

    return df.filter(col('birthYear').isNotNull()) \
             .withColumn('rank', rank().over(window_spec)) \
             .filter(col('rank') == 1) \
             .drop('rank')