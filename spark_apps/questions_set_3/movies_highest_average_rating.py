from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_3_1")
    ratings_df = sp_utils.read_data(spark, "title.ratings.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    combined_df = ratings_df.join(basics_df, ratings_df.tconst == basics_df.tconst)

    window_spec = Window.orderBy(desc("averageRating"))
    top_movies = combined_df.filter(col("numVotes") > 100000) \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 10) \
        .select("primaryTitle", "averageRating", "numVotes")

    sp_utils.save_dataframe(top_movies, "results_3_1")
    spark.stop()


if __name__ == "__main__":
    main()
