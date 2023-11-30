from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os

INPUT_DATA_PARENT_DIR = '/opt/data'
OUTPUT_DATA_PARENT_DIR = '/opt/data/output'


def create_spark_session(app_name) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_data(spark, filename, sep="\t") -> DataFrame:
    file_path = os.path.join(INPUT_DATA_PARENT_DIR, filename)
    return spark.read.option("sep", sep).csv(file_path, header=True, inferSchema=True)


def join_dataframes(df_1, df_2, by_column_name):
    return df_1.join(df_2, by_column_name)


def save_dataframe(df, output_dir_name):
    full_dir_path = os.path.join(OUTPUT_DATA_PARENT_DIR, output_dir_name)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(full_dir_path)
