from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_3")
    principals_df = sp_utils.read_data(spark, "title.principals.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    names_df = sp_utils.read_data(spark, "name.basics.tsv")

    tv_series = basics_df.filter(F.col("titleType") == "tvSeries")
    tv_series_actors = tv_series.join(principals_df, tv_series.tconst == principals_df.tconst) \
        .filter(F.col("category") == "actor")
    actor_series_count = tv_series_actors.groupBy("nconst").agg(
        F.countDistinct(tv_series["tconst"]).alias("series_count"))
    tv_series_actors_names = actor_series_count.join(names_df, tv_series_actors.nconst == names_df.nconst) \
        .select("primaryName", "series_count") \
        .orderBy(F.desc("series_count")).limit(20)

    sp_utils.save_dataframe(tv_series_actors_names, "results_3_3")
    spark.stop()


if __name__ == "__main__":
    main()
