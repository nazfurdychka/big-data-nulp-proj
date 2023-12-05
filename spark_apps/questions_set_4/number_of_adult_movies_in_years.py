import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_4_5")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    # Check the data type of the "genres" column
    if not isinstance(basics_df.schema["genres"].dataType, ArrayType):
        # If it's not already an array, try to split the string to convert to an array
        basics_df = basics_df.withColumn("genres", F.split(basics_df["genres"], ",").cast(ArrayType(StringType())))

    # Count the total number of adult movies in the entire dataset
    total_adult_movies_count = basics_df.filter(F.array_contains(basics_df["genres"], "Adult")).count()
    print(f"Total number of adult movies in the entire dataset: {total_adult_movies_count}")

    # Count the number of adult movies for each year
    adult_movies_by_year_df = basics_df.filter(F.array_contains(basics_df["genres"], "Adult"))\
        .groupBy("startYear").agg(F.count("*").alias("count"))

    # Save the DataFrame to a CSV file
    sp_utils.save_dataframe(adult_movies_by_year_df, "results_4_5_adult_movies_by_year")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
