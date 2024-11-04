from setting import RATINGS_FILE_PATH
from setting import RESULTS_PACKAGE_PATH
import pyspark.sql.types as t

def read_ratings_df(path, spark_session):
    df_schema = t.StructType([
        t.StructField("ID", t.StringType(), False),
        t.StructField("AverageRating", t.DoubleType(), True),
        t.StructField("VotesNumber", t.IntegerType(), True)
    ])

    read_df = spark_session.read.csv(path,
                                     header=False,
                                     sep="\t",
                                     schema=df_schema,
                                     nullValue= "null",
                                     nanValue="NaN",
                                     mode="DROPMALFORMED",
                                     inferSchema=True,
                                     enforceSchema=False,
                                     multiLine=True,
                                     )
    return read_df

def write_ratings_df_to_csv(df,path):
    df.write.csv(
        path,
        header=True,
        mode="overwrite",
        quote='"'
    )
    print(f"DataFrame записано у {path}")