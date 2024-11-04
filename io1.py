from setting import RATINGS_FILE_PATH

def read_ratings_df(path, spark_session):
    read_df = spark_session.read.csv(path, header=True)
    return read_df