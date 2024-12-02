# Не використовувати Pandas та UDF
# Без collect() та toPandas()
"""
from basic_dfs.basic_dfs_moshkovskyi import basic_test_df

def view_dataframe():
    df = basic_test_df()
    df.show()

view_dataframe()
"""

import pyspark.sql.types as t
from pyspark.sql import SparkSession

from agregation_sorting_and_window_functions import most_active_director, movies_with_three_genres, count_program_types, \
    longest_tv_show, earliest_born_person
from filtering import count_adult_movies, top_n_highest_rated, top_n_most_voted
from io1 import read_ratings_df, write_ratings_df_to_csv
from io1 import RATINGS_FILE_PATH
from io1 import RESULTS_PACKAGE_PATH
from nulls_and_duplicates_editing import drop_writers_column, drop_characters_column, drop_job_column, \
    drop_language_column, fill_null_start_year, title_duplicates_removal, ratings_duplicates_removal
from title_episode_editing import transform_title_episode
from title_ratings_editing import transform_title_ratings
from title_crew_editing import transform_title_crew
from title_principals_editing import transform_title_principals
from name_basics_editing import transform_name_basics
from title_akas_editing import transform_title_akas
from title_basics_editing import transform_title_basics

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("ratings_dfs")
                 .getOrCreate()
                )

df_ratings = read_ratings_df(RATINGS_FILE_PATH, spark_session)
df_transformed = transform_title_ratings(df_ratings)
write_ratings_df_to_csv(df_transformed, RESULTS_PACKAGE_PATH)

df_crew = spark_session.read.option("delimiter", "\t").csv("data/title.crew.tsv.gz", header=True)
df_crew_transformed = transform_title_crew(df_crew)
write_ratings_df_to_csv(df_crew_transformed, RESULTS_PACKAGE_PATH)

df_principals = spark_session.read.option("delimiter", "\t").csv("data/title.principals.tsv.gz", header=True)
df_principals_transformed = transform_title_principals(df_principals)
write_ratings_df_to_csv(df_principals_transformed, RESULTS_PACKAGE_PATH)

df_basics = spark_session.read.option("delimiter", "\t").csv("data/name.basics.tsv.gz", header=True)
df_basics_transformed = transform_name_basics(df_basics)
write_ratings_df_to_csv(df_basics_transformed, RESULTS_PACKAGE_PATH)

df_akas = spark_session.read.option("delimiter", "\t").csv("data/title.akas.tsv.gz", header=True)
df_akas_transformed = transform_title_akas(df_akas)
write_ratings_df_to_csv(df_akas_transformed, RESULTS_PACKAGE_PATH)

df_episode = spark_session.read.option("delimiter", "\t").csv("data/title.episode.tsv.gz", header=True)
df_episode_transformed = transform_title_episode(df_episode)
write_ratings_df_to_csv(df_episode_transformed, RESULTS_PACKAGE_PATH)

df_title_basics = spark_session.read.option("delimiter", "\t").csv("data/title.basics.tsv.gz", header=True)
df_title_basics_transformed = transform_title_basics(df_title_basics)
write_ratings_df_to_csv(df_title_basics_transformed, RESULTS_PACKAGE_PATH)

"""
Nulls and duplicates editing
"""
df_crew_edited = drop_writers_column(df_crew_transformed)
write_ratings_df_to_csv(df_crew_edited, RESULTS_PACKAGE_PATH)

df_principals_edited = drop_characters_column(df_principals_transformed)
df_principals_edited_2 = drop_job_column(df_principals_edited)
write_ratings_df_to_csv(df_principals_edited_2, RESULTS_PACKAGE_PATH)

df_akas_edited = drop_language_column(df_akas_transformed)
write_ratings_df_to_csv(df_akas_edited, RESULTS_PACKAGE_PATH)

df_title_basics_edited = fill_null_start_year(df_title_basics_transformed)

df_title_basics_edited1 = title_duplicates_removal(df_title_basics_edited)
write_ratings_df_to_csv(df_title_basics_edited1, RESULTS_PACKAGE_PATH)

df_ratings_edited = ratings_duplicates_removal(df_transformed)
write_ratings_df_to_csv(df_ratings_edited, RESULTS_PACKAGE_PATH)

"""
Business questions realisation with filtering
"""
"Question 1"
adult_movie_count = count_adult_movies(df_title_basics_edited1)
print(f"Number of adult movies: {adult_movie_count}")

"Question 3"
top_rated = top_n_highest_rated(df_ratings_edited)
write_ratings_df_to_csv(top_rated, RESULTS_PACKAGE_PATH)

"Question 4"
top_voted = top_n_most_voted(df_ratings_edited)
write_ratings_df_to_csv(top_voted, RESULTS_PACKAGE_PATH)

"""
Business questions realisation with aggregation, sorting and window functions
"""

"Question 5"
most_films_directors = most_active_director(df_crew_edited)
write_ratings_df_to_csv(most_films_directors, RESULTS_PACKAGE_PATH)

"Question 9"
three_genres_movies = movies_with_three_genres(df_title_basics_edited1)
print(f"Number of movies with exactly three genres: {three_genres_movies}")

"Question 6"
distinct_program_types = count_program_types(df_title_basics_edited)
print(f"Number of distinct program types: {distinct_program_types}")

"Question 7"
print("Longest-running TV show:")
lts_df = longest_tv_show(df_title_basics_edited1)
write_ratings_df_to_csv(lts_df, RESULTS_PACKAGE_PATH)

"Question 2"
print("Earliest born person:")
elp_df = earliest_born_person(df_basics_transformed)
write_ratings_df_to_csv(elp_df, RESULTS_PACKAGE_PATH)








