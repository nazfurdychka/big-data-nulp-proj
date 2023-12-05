import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import Window

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    # Step 1: Initialize SparkSession
    spark = sp_utils.create_spark_session("DirectorWriterCombination")

    # Step 2: Load the required datasets
    title_basics = sp_utils.read_data(spark, "title.basics.tsv")
    title_ratings = sp_utils.read_data(spark, "title.ratings.tsv")

    # Step 3: Join the datasets on 'tconst'
    joined_data = title_basics.join(title_ratings, "tconst")

    # Step 4: Filter out missing or null values for genres and averageRating
    filtered_data = joined_data.filter(
        joined_data["genres"].isNotNull() & joined_data["averageRating"].isNotNull()
    )

    # Step 5: Group by genres, calculate average rating and count occurrences
    genre_stats = filtered_data.groupBy("genres") \
        .agg(F.count("*").alias("title_count"), F.avg("averageRating").alias("avg_rating"))

    # Step 6: Rank by average rating in descending order
    window_spec = Window.orderBy(F.desc("avg_rating"))
    ranked_genres = genre_stats.withColumn("rank", F.rank().over(window_spec))

    # Step 7: Filter for the top-ranked genre combinations
    top_genres = ranked_genres.filter("rank == 1").drop("rank")

    # Step 8: Show the results
    top_genres.show(truncate=False)

    # Step 9: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
