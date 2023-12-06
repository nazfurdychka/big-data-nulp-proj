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
    spark = sp_utils.create_spark_session("LanguagesByRegion")

    # Step 2: Load the required datasets
    title_akas = sp_utils.read_data(spark, "title.akas.tsv")
    title_basics = sp_utils.read_data(spark, "title.basics.tsv")

    # Step 3: Join the datasets on 'titleId'
    joined_data = title_akas.join(title_basics, title_akas.titleId == title_basics.tconst)

    # Step 4: Filter out missing or null values for language and region
    filtered_data = joined_data.filter(
        (F.col("language") != sp_utils.NA_VALUE) & (F.col("region") != sp_utils.NA_VALUE)
    )

    # Step 5: Group by region and language, count occurrences
    language_counts = filtered_data.groupBy("region", "language").agg(F.count("*").alias("title_count"))

    # Step 6: Find the most common language for each region
    window_spec = Window.partitionBy("region").orderBy(F.desc("title_count"))
    most_common_language = language_counts.withColumn("rank", F.rank().over(window_spec)).filter("rank == 1").drop("rank")
    most_common_language = most_common_language.orderBy(F.desc('title_count'))

    # Step 7: Show the results
    most_common_language.show(truncate=False)

    # Step 8: Save results
    sp_utils.save_dataframe(most_common_language, "most_common_language")

    # Step 9: Stop the SparkSession
    spark.stop()


if __name__ == '__main__':
    main()
