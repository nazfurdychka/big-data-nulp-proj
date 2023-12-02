from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_2")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_df = sp_utils.read_data(spark, "title.ratings.tsv")

    combined_df = sp_utils.join_dataframes(basics_df, ratings_df, "tconst")

    total_ratings_per_type = combined_df.groupBy("titleType").agg(F.sum("numVotes").alias("total_ratings"))
    overall_total_ratings = total_ratings_per_type.select(
        F.sum("total_ratings").alias("overall_total")).first().overall_total
    percentage_distribution = total_ratings_per_type.withColumn("percentage",
                                                                F.col("total_ratings") / overall_total_ratings * 100)

    sp_utils.save_dataframe(percentage_distribution, "results_1_2")
    spark.stop()


if __name__ == "__main__":
    main()
