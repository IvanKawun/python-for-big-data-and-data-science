# Не використовувати Pandas та UDF
# Без collect() та toPandas()

from basic_dfs.basic_dfs_moshkovskyi import basic_test_df

def view_dataframe():
    df = basic_test_df()
    df.show()

view_dataframe()