from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_5")
    ratings_df = sp_utils.read_data(spark, "title.ratings.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    blockbuster_movies = ratings_df.join(basics_df, ratings_df.tconst == basics_df.tconst) \
        .filter((F.col("titleType") == "movie") & (F.col("averageRating") > 8))

    blockbuster_movies_per_year = blockbuster_movies.groupBy("startYear") \
        .agg(F.count("*").alias("blockbuster_count")) \
        .orderBy(F.desc("blockbuster_count")).limit(3)

    sp_utils.save_dataframe(blockbuster_movies_per_year, "results_3_5")
    spark.stop()


if __name__ == "__main__":
    main()
