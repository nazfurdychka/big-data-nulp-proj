from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_4")
    akas_df = sp_utils.read_data(spark, "title.akas.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    movies_languages = akas_df.join(basics_df, akas_df.titleId == basics_df.tconst) \
        .filter(F.col("titleType") == "movie") \
        .select("primaryTitle", "language") \
        .distinct()

    movie_languages_count = movies_languages.groupBy("primaryTitle") \
        .agg(F.countDistinct("language").alias("language_count")) \
        .filter(F.col("language_count") >= 3)

    sp_utils.save_dataframe(movie_languages_count, "results_3_4")
    spark.stop()


if __name__ == "__main__":
    main()
