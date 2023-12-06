import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode, col

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils

release_year = 2023
target_genres = ["Comedy", "Fantasy", ""]


def main():
    spark = sp_utils.create_spark_session("Session_4_6")

    # Read the basics and ratings data
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_df = sp_utils.read_data(spark, "title.ratings.tsv")

    # Filter for movies released before or in the max year
    basics_df = basics_df.filter(col("primaryTitle") != "\\N").filter(col("startYear") == release_year)

    # Join the datasets on titleID
    joined_df = basics_df.join(ratings_df, on="tconst", how="inner")

    # Check and handle genre data type
    if not isinstance(joined_df.schema["genres"].dataType, ArrayType):
        joined_df = joined_df.withColumn("genres", F.split(joined_df["genres"], ",").cast(ArrayType(StringType())))

    for i in range(0, len(target_genres)):
        # Filter movies for the specific genre and select the top 20 by votes
        top_movies_df = joined_df.filter(F.array_contains("genres", target_genres[i]))\
            .orderBy(F.desc("numVotes")).limit(20)\
            .select("tconst", "primaryTitle", "numVotes", "averageRating", F.concat_ws(",", "genres").alias("genres_str"))

        # Save the results
        sp_utils.save_dataframe(top_movies_df, f"results_4_6_top_movies_{target_genres[i]}")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
