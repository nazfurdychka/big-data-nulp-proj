from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_2")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    movies_2022 = basics_df.filter((F.col("titleType") == "movie") & (F.col("startYear") == 2022))
    movies_2022_split = movies_2022.withColumn("genres_array", F.split("genres", ","))
    movies_2022_exploded = movies_2022_split.select("tconst", F.explode("genres_array").alias("genre"))
    genre_count = movies_2022_exploded.groupBy("genre").agg(F.count("*").alias("movie_count")) \
        .orderBy(F.desc("movie_count")).limit(5)

    sp_utils.save_dataframe(genre_count, "results_3_2")
    spark.stop()


if __name__ == "__main__":
    main()
