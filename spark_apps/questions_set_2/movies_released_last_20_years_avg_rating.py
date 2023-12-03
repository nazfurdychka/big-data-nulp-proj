from pyspark.sql.functions import col

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_2")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")

    filtered_titles_df = basics_data.filter(
        (col("titleType") == "movie") &
        (col("startYear").isNotNull()) &
        (col("startYear") >= 2000) &
        (col("startYear") <= 2022)
    )

    ratings_df = ratings_data.withColumnRenamed("tconst", "ratings_tconst")

    joined_df = filtered_titles_df.join(
        ratings_df,
        filtered_titles_df["tconst"] == ratings_df["ratings_tconst"]
    )

    result_df = joined_df.groupBy("startYear").agg({"ratings_tconst": "count", "averageRating": "avg"})

    result_df = result_df.orderBy("startYear")

    sp_utils.save_dataframe(result_df, "results_2_2")
    spark.stop()


if __name__ == "__main__":
    main()
