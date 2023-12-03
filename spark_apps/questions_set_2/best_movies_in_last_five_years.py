import pyspark.sql.functions as F
from pyspark.sql.window import Window

import os
import sys
import datetime

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_5")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")

    basics_filtered = basics_data.filter(
        (basics_data["titleType"] == "movie") & (basics_data["startYear"].isNotNull())
    )
    ratings_filtered = ratings_data.filter(
        (ratings_data["averageRating"] > 7) & (ratings_data["numVotes"] > 0)
    )

    basics_ratings_data = basics_filtered.join(ratings_filtered, "tconst")
    basics_ratings_data = basics_ratings_data.withColumn("decade", F.floor(basics_ratings_data["startYear"] / 10) * 10)

    current_year = datetime.date.today().year

    basics_ratings_last_5_decades = basics_ratings_data.filter((current_year - basics_ratings_data["startYear"] < 50))

    window_spec = Window.orderBy(F.desc("averageRating"))

    ranked_data = basics_ratings_last_5_decades.withColumn("rank", F.rank().over(window_spec))

    top_3_movies = ranked_data.filter("rank <= 3")

    final_result = top_3_movies.select("primaryTitle", "startYear", "averageRating")

    sp_utils.save_dataframe(final_result, "results_2_5")
    spark.stop()


if __name__ == "__main__":
    main()
