import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_4_1")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    # Check the data type of the "genres" column
    if not isinstance(basics_df.schema["genres"].dataType, ArrayType):
        # If it's not already an array, try to split the string to convert to an array
        basics_df = basics_df.withColumn("genres", F.split(basics_df["genres"], ",").cast(ArrayType(StringType())))

    # Explode the genres array to have one genre per row
    exploded_df = basics_df.select(explode("genres").alias("genre"))

    # Drop duplicates to get unique genres
    unique_genres_df = exploded_df.select("genre").distinct()

    # Save the DataFrame to a CSV file
    sp_utils.save_dataframe(unique_genres_df, "results_4_1_unique_genres")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
