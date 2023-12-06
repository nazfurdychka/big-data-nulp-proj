from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode, collect_set, row_number, col
from pyspark.sql.window import Window

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_4_2")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_df = sp_utils.read_data(spark, "title.ratings.tsv")

    # Check the data type of the "genres" column
    if not isinstance(basics_df.schema["genres"].dataType, ArrayType):
        # If it's not already an array, try to split the string to convert to an array
        basics_df = basics_df.withColumn("genres", F.split(basics_df["genres"], ",").cast(ArrayType(StringType())))

    # Explode the genres array to have one genre per row
    exploded_df = basics_df.select("tconst", "primaryTitle", "originalTitle", explode("genres").alias("genre"))

    # Join with basics_df to get the ratings for each movie
    joined_df = exploded_df.join(ratings_df, on="tconst", how="inner")

    # Define a Window specification for each genre to rank movies based on averageRating
    window_spec = Window.partitionBy("genre").orderBy(col("averageRating").desc())

    # Add a row number based on the rating, partitioned by genre
    ranked_df = joined_df.withColumn("row_num", row_number().over(window_spec))

    # Filter out the top 5 rated movies for each genre
    top_5_movies_by_genre = ranked_df.filter(col("row_num") <= 5)

    # Save the unique genres DataFrame to a CSV file
    sp_utils.save_dataframe(top_5_movies_by_genre, "results_4_2_top_5_movies_by_genre")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
