import pyspark.sql.functions as F
from pyspark.sql.window import Window

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_4")
    principals_data = sp_utils.read_data(spark, "title.principals.tsv")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")
    names_data = sp_utils.read_data(spark, "name.basics.tsv")

    principals_filtered = principals_data.filter(
        (principals_data["category"] == "actor") | (principals_data["category"] == "actress")
    )
    basics_filtered = basics_data.filter(
        (F.col("titleType") == "tvSeries") & (F.col("endYear") != "\\N") & (F.col("startYear") != "\\N")
    )

    joined_data = (
        principals_filtered
        .join(names_data, "nconst")
        .join(basics_filtered, "tconst")
        .join(ratings_data, "tconst")
    )

    grouped_data = joined_data.groupBy("nconst", "primaryName").agg(
        F.count("tconst").alias("episode_count"),
        F.avg("averageRating").alias("average_rating")
    )

    window_spec_1 = Window.orderBy(F.desc("episode_count"))
    ranked_data = grouped_data.withColumn("rank", F.rank().over(window_spec_1))

    top_5_data = ranked_data.filter("rank <= 5")

    window_spec_2 = Window.orderBy(F.desc("average_rating"))
    final_result = top_5_data.withColumn("rank", F.rank().over(window_spec_2))

    sp_utils.save_dataframe(final_result, "results_2_4")
    spark.stop()


if __name__ == "__main__":
    main()
