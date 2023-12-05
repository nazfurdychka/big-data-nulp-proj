from pyspark.sql.functions import col, count, when, dense_rank
from pyspark.sql.window import Window
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_6")
    principals_df = sp_utils.read_data(spark, "title.principals.tsv")
    basics_df = sp_utils.read_data(spark, "name.basics.tsv")

    actor_df = principals_df.filter(col("category") == "actor")
    actress_df = principals_df.filter(col("category") == "actress")

    actor_actress_pairs_df = actor_df.alias("a").join(
        actress_df.alias("b"),
        (col("a.tconst") == col("b.tconst")),
        "inner"
    ).select(
        col("a.nconst").alias("actor_nconst"),
        col("b.nconst").alias("actress_nconst"),
        col("a.tconst").alias("movie_id")
    )

    pair_movie_count_df = actor_actress_pairs_df.groupBy("actor_nconst", "actress_nconst").agg(
        count("movie_id").alias("movie_count"))

    pair_movie_count_with_names_df = pair_movie_count_df.join(
        basics_df.select("nconst", "primaryName").alias("actor_names"),
        col("actor_nconst") == col("actor_names.nconst"),
        "left"
    ).join(
        basics_df.select("nconst", "primaryName").alias("actress_names"),
        col("actress_nconst") == col("actress_names.nconst"),
        "left"
    ).select(
        col("actor_names.primaryName").alias("actor_name"),
        col("actress_names.primaryName").alias("actress_name"),
        col("movie_count")
    )

    window_spec = Window.orderBy(col("movie_count").desc())
    ranked_pairs_df = pair_movie_count_with_names_df.withColumn("rank", when(col("movie_count") > 1,
                                                                             dense_rank().over(window_spec)))
    top_pairs_df = ranked_pairs_df.filter(col("rank") <= 10).select("actor_name", "actress_name", "movie_count")

    sp_utils.save_dataframe(top_pairs_df, "results_3_6")
    spark.stop()


if __name__ == "__main__":
    main()
