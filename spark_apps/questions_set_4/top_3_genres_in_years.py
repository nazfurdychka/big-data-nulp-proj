import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode
from pyspark.sql.window import Window

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_4_3")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    rating_df = sp_utils.read_data(spark, "title.ratings.tsv")

    # Check the data type of the "genres" column
    if not isinstance(basics_df.schema["genres"].dataType, ArrayType):
        # If it's not already an array, try to split the string to convert to an array
        basics_df = basics_df.withColumn("genres", F.split(basics_df["genres"], ",").cast(ArrayType(StringType())))

    # Explode the genres array to have one genre per row
    exploded_df = basics_df.select("startYear", explode("genres").alias("genre"))

    # Drop duplicates to get unique genres for each year
    unique_genres_by_year_df = exploded_df.groupBy("startYear", "genre").count()

    # Use window function to rank genres by count within each year
    window_spec = Window.partitionBy("startYear").orderBy(F.desc("count"))
    ranked_genres_df = unique_genres_by_year_df.withColumn("rank", F.row_number().over(window_spec))

    # Select top 3 genres for each year
    top3_genres_df = ranked_genres_df.filter("rank <= 3").select("startYear", "genre")

    # Save the DataFrame to a CSV file
    sp_utils.save_dataframe(top3_genres_df, "results_4_3_top3_genres_by_year")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
