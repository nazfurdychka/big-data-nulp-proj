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
    spark = sp_utils.create_spark_session("GenresByRegion")

    # Step 2: Load the required datasets
    title_akas = sp_utils.read_data(spark, "title.akas.tsv")
    title_basics = sp_utils.read_data(spark, "title.basics.tsv")

    # Step 3: Join the datasets on 'titleId' and 'tconst'
    joined_data = title_akas.join(title_basics, title_akas.titleId == title_basics.tconst)

    joined_data.show()

    # Step 4: Filter out missing or null values for genres, region, and language
    filtered_data = joined_data.filter(
        (joined_data["genres"] != sp_utils.NA_VALUE) &
        (joined_data["region"] != sp_utils.NA_VALUE)
    )

    filtered_data.show()

    # Step 5: Group by region, language, and genres, count occurrences
    genre_counts = filtered_data.groupBy("region", "genres").agg(F.count("*").alias("title_count"))

    genre_counts.show()

    # Step 6: Rank by title count in descending order
    window_spec = Window.partitionBy("region").orderBy(F.desc("title_count"))
    ranked_genres = genre_counts.withColumn("rank", F.rank().over(window_spec))

    ranked_genres.show()

    # Step 7: Filter for the top-ranked genre in each region and language
    top_genres = ranked_genres.filter("rank == 1").drop("rank")

    # Step 8: Show the results
    top_genres.show(truncate=False)

    # Step 9: Save the results
    sp_utils.save_dataframe(top_genres, 'most_popular_genres_by_region')

    # Step 10: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
