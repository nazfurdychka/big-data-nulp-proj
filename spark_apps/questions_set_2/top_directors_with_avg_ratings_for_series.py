from pyspark.sql.functions import split, col, explode

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_3")
    crew_data = sp_utils.read_data(spark, "title.crew.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")
    names_data = sp_utils.read_data(spark, "name.basics.tsv")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")

    filtered_titles_df = basics_data.filter(
        (col("titleType") == "tvSeries") & (col("endYear") != "\\N") & (col("startYear") != "\\N")
    )

    title_crew_df = crew_data.withColumn("director_ids", split(col("directors"), ","))

    title_crew_df = title_crew_df.withColumn("director_id", explode(col("director_ids")))

    joined_df = title_crew_df\
        .join(
            ratings_data, title_crew_df["tconst"] == ratings_data["tconst"]
        )\
        .join(
            filtered_titles_df,title_crew_df["tconst"] == filtered_titles_df["tconst"]
        )\
        .join(
            names_data, names_data["nconst"] == title_crew_df["director_id"]
        )

    directors_ratings_df = joined_df.select(col("director_id"), col("primaryName"), col("averageRating"))\
        .groupBy("director_id", "primaryName")\
        .agg({"averageRating": "avg"})

    top_10_directors = directors_ratings_df.orderBy("avg(averageRating)", ascending=False).limit(10)

    top_10_directors.show(truncate=False)

    sp_utils.save_dataframe(top_10_directors, "results_2_3")
    spark.stop()


if __name__ == "__main__":
    main()
