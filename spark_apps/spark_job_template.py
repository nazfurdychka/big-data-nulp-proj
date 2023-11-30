from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
sys.path.extend([parent_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("TEST")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")

    joined_df = sp_utils.join_dataframes(basics_data, ratings_data, "tconst")

    total_ratings_per_type = joined_df.groupBy("titleType").agg(F.sum("numVotes").alias("total_ratings"))
    overall_total_ratings = total_ratings_per_type.select(
        F.sum("total_ratings").alias("overall_total")).first().overall_total
    percentage_distribution = total_ratings_per_type.withColumn("percentage",
                                                                F.col("total_ratings") / overall_total_ratings * 100)

    percentage_distribution.show(10)

    sp_utils.save_dataframe(percentage_distribution, "some_test")

    spark.stop()


if __name__ == "__main__":
    main()
