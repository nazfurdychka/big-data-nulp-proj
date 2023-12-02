from pyspark.sql import functions as F
from pyspark.sql import Window
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_5")
    principals_data = sp_utils.read_data(spark, "title.principals.tsv")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    ratings_data = sp_utils.read_data(spark, "title.ratings.tsv")
    names_data = sp_utils.read_data(spark, "name.basics.tsv")

    windowSpec = Window.orderBy(F.col("averageRating").desc())
    ranked_ratings_df = ratings_data.withColumn("rank", F.percent_rank().over(windowSpec))
    top_rated_movies = ranked_ratings_df.filter(F.col("rank") <= 0.1)

    top_rated_movies_details = top_rated_movies.join(basics_data, "tconst")
    actors_in_top_movies = principals_data.filter(principals_data["category"] == "actor").join(top_rated_movies_details,
                                                                                           "tconst")
    actor_details = actors_in_top_movies.join(names_data, actors_in_top_movies["nconst"] == names_data["nconst"])
    actor_details = actor_details.withColumn("age", actor_details["startYear"].cast("integer") - actor_details[
        "birthYear"].cast("integer"))
    average_age = actor_details.agg(F.avg("age")).limit(1)

    sp_utils.save_dataframe(average_age, "results_1_5")
    spark.stop()



if __name__ == "__main__":
    main()
