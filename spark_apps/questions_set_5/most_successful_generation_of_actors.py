import os
import sys

from pyspark.sql import functions as F

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    # Step 1: Initialize SparkSession
    spark = sp_utils.create_spark_session("ActorGenerationSuccess")
    # Step 2: Load the required datasets
    name_basics = sp_utils.read_data(spark, 'name.basics.tsv')
    title_principals = sp_utils.read_data(spark, 'title.principals.tsv')
    title_ratings = sp_utils.read_data(spark, 'title.ratings.tsv')
    # Step 3: Join the datasets on 'nconst' and 'tconst'
    joined_data = title_principals.join(name_basics, "nconst").join(title_ratings, "tconst")
    # Step 4: Calculate age by subtracting birthYear from the deathYear
    age_data = joined_data.withColumn("actor_age",
                                      F.when(((F.col("birthYear") != sp_utils.NA_VALUE) &
                                              (F.col("deathYear") != sp_utils.NA_VALUE)),
                                             F.col("deathYear").cast("int") - F.col("birthYear").cast("int")))
    # Step 5: Categorize actors into generations (e.g., by decades)
    age_data = age_data.withColumn("generation", F.expr('(actor_age div 10) * 10'))
    # Step 6: Filter out missing or null values for age and averageRating
    filtered_data = age_data.filter(
        age_data["actor_age"].isNotNull() & age_data["averageRating"].isNotNull())

    # Step 7: Group by generation, calculate average rating and count occurrences
    generation_stats = filtered_data.groupBy("generation") \
        .agg(F.count("*").alias("title_count"), F.avg("averageRating").alias("avg_rating")).orderBy(F.desc('avg_rating'))
    # Step 8: Show the results
    generation_stats.show(truncate=False)
    # Step 9: Save the results
    sp_utils.save_dataframe(generation_stats, 'most_successful_generation_of_actors')
    # Step 10: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
