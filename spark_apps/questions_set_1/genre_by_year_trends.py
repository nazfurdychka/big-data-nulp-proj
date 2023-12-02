from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_3")
    basics_data = sp_utils.read_data(spark, "title.basics.tsv")
    current_year = F.year(F.current_date())
    recent_movies_df = basics_data.filter(
        (F.col("titleType") == "movie") &
        (F.col("startYear") != sp_utils.NA_VALUE) &
        (F.col("genres") != sp_utils.NA_VALUE) &
        (F.col("startYear").cast("int") >= (current_year - 10)))
    exploded_genres_df = recent_movies_df.withColumn("genre", F.explode(F.split(F.col("genres"), ",")))
    genre_year_counts = exploded_genres_df.groupBy("genre", "startYear").count()

    top_genres = genre_year_counts.groupBy("genre").sum("count").orderBy("sum(count)", ascending=False).limit(1).select(
        "genre")
    yearly_counts = genre_year_counts.join(top_genres, "genre")

    window_spec = Window.partitionBy("genre").orderBy("startYear")
    yearly_counts = yearly_counts.withColumn("prev_count", F.lag("count").over(window_spec))
    yearly_counts = yearly_counts.withColumn("percentage_change",
                                             (F.col("count") - F.col("prev_count")) / F.col("prev_count") * 100)
    filtered_yearly_counts = yearly_counts.filter(
        (F.col("startYear") >= (current_year - 10)) &
        (F.col("startYear") <= current_year)).select("genre", "startYear", "count", "percentage_change")

    sp_utils.save_dataframe(filtered_yearly_counts, "results_1_3")
    spark.stop()


if __name__ == "__main__":
    main()
