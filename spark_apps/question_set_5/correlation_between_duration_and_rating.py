import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    # Step 1: Initialize SparkSession
    spark = sp_utils.create_spark_session("TitleRatingCorrelation")

    # Step 2: Load the required datasets
    title_basics = sp_utils.read_data(spark, "title.basics.tsv")
    title_ratings = sp_utils.read_data(spark, "title.ratings.tsv")

    # Step 3: Join the datasets
    title_data = title_basics.join(title_ratings, "tconst")

    # Step 4: Filter out missing or null values
    title_data = title_data.filter(title_data["runtimeMinutes"].isNotNull() & title_data["averageRating"].isNotNull())

    # Step 5: Convert columns to appropriate data types
    title_data = title_data.withColumn("runtimeMinutes", title_data["runtimeMinutes"].cast("int"))
    title_data = title_data.withColumn("averageRating", title_data["averageRating"].cast("float"))

    # Step 6: Calculate correlation
    correlation = title_data.stat.corr("runtimeMinutes", "averageRating")

    # Step 7: Print the correlation result
    print(f"Correlation between runtime and average rating: {correlation}")

    # Step 8: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
