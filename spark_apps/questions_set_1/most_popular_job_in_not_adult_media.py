from pyspark.sql import functions as F
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_4")
    principals_df = sp_utils.read_data(spark, "title.principals.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")

    not_adult_media_df = basics_df.filter(F.col("isAdult") == 0)

    combined_df = not_adult_media_df.join(principals_df, "tconst")
    combined_df = combined_df.filter((F.col("job") != sp_utils.NA_VALUE))
    job_counts = combined_df.groupBy("job").count()

    most_popular_job = job_counts.orderBy(F.col("count").desc()).limit(1)
    sp_utils.save_dataframe(most_popular_job, "results_1_4")
    spark.stop()


if __name__ == "__main__":
    main()
