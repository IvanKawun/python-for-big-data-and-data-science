from pyspark.sql import DataFrame
def count_adult_movies(df: DataFrame) -> int:
    """
    Counts the number of adult movies.
    :param df: DataFrame containing data from title.basics.tsv
    :return: Number of adult movies
    """
    return df.filter(df['is_adult'] == 1).count()

def top_n_highest_rated(df: DataFrame, n: int = 10) -> DataFrame:
    """
    Returns the top N movies with the highest ratings.
    :param df: DataFrame containing data from title.ratings.tsv
    :param n: Number of top movies to return
    :return: DataFrame with the top N highest-rated movies
    """
    return df.orderBy(df['average_rating'].desc()).limit(n)

def top_n_most_voted(df: DataFrame, n: int = 20) -> DataFrame:
    """
    Returns the top N movies with the highest number of votes.
    :param df: DataFrame containing data from title.ratings.tsv
    :param n: Number of top movies to return
    :return: DataFrame with the top N most-voted movies
    """
    return df.orderBy(df['votes_number'].desc()).limit(n)