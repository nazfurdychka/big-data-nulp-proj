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
    title_crew = sp_utils.read_data(spark, 'title.crew.tsv')
    title_ratings = sp_utils.read_data(spark, 'title.ratings.tsv')
    name_basics = sp_utils.read_data(spark, 'name.basics.tsv')

    # Step 3: Filter datasets
    title_crew = title_crew.filter((F.col('directors') != sp_utils.NA_VALUE) &
                                   (F.col('writers') != sp_utils.NA_VALUE))
    title_ratings = title_ratings.filter(F.col('averageRating').cast('string') != sp_utils.NA_VALUE)

    # Step 4: Decompose arrays in title_crew columns
    title_crew = title_crew.withColumn('writers', F.split(title_crew['writers'], ','))
    title_crew = title_crew.withColumn('directors', F.split(title_crew['directors'], ','))
    title_crew = title_crew.selectExpr('tconst', 'directors', 'explode(writers) as writer')
    title_crew = title_crew.selectExpr('tconst', 'explode(directors) as director', 'writer')

    # Step 5: Join the datasets on 'tconst'
    joined_data = title_crew.join(title_ratings, "tconst")

    # Step 6: Group by directors and writers, calculate average rating and count occurrences
    director_writer_stats = joined_data.groupBy("director", "writer") \
        .agg(F.count("*").alias("title_count"), F.avg("averageRating").alias("avg_rating"))

    # Step 7: Rank by average rating in descending order
    window_spec = Window.orderBy(F.desc("avg_rating"))
    ranked_combinations = director_writer_stats.withColumn("rank", F.rank().over(window_spec))

    # Step 8: Filter for the top-ranked combination
    top_combination = ranked_combinations.filter("rank == 1").drop("rank")

    # Step 9: Join with names
    top_combination = top_combination.join(
        name_basics, F.col('writer') == F.col('nconst')
    ).select('director', F.col('primaryName').alias('writer'), 'title_count', 'avg_rating')
    top_combination = top_combination.join(
        name_basics, F.col('director') == F.col('nconst')
    ).select(F.col('primaryName').alias('director'), 'writer', 'title_count', 'avg_rating')

    # Step 10: Filter for the authors with more than three titles
    top_combination = top_combination.filter(F.col('title_count') > 3)

    # Step 11: Show the results
    top_combination.show(truncate=False)

    # Step 12: Save the results
    sp_utils.save_dataframe(top_combination, 'most_successful_directors_and_writers')

    # Step 13: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
