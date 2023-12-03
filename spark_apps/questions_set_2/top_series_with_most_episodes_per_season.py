from pyspark.sql.functions import col

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_1")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    episodes_data = sp_utils.read_data(spark, "title.episode.tsv")

    filtered_titles_df = basics_data.filter(
        (col("titleType") == "tvSeries") & (col("endYear") != "\\N") & (col("startYear") != "\\N")
    )

    joined_df = filtered_titles_df.join(
        episodes_data.withColumnRenamed("tconst", "episode_tconst"),
        filtered_titles_df["tconst"] == episodes_data["parentTconst"]
    )

    avg_episodes_df = joined_df.groupBy("tconst", "primaryTitle", "startYear", "endYear", "seasonNumber")\
        .agg({"episodeNumber": "count"})

    filtered_avg_episodes_df = avg_episodes_df.filter(avg_episodes_df["seasonNumber"] >= 2)

    avg_episodes_per_season_df = filtered_avg_episodes_df.groupBy("tconst", "primaryTitle", "startYear", "endYear")\
        .agg({"count(episodeNumber)": "avg"})

    top_10_tv_series = avg_episodes_per_season_df.orderBy("avg(count(episodeNumber))", ascending=False).limit(10)

    sp_utils.save_dataframe(top_10_tv_series, "results_2_1")
    spark.stop()


if __name__ == "__main__":
    main()
