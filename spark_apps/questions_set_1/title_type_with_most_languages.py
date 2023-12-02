from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_6")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    akas_df = sp_utils.read_data(spark, "title.akas.tsv")
    akas_df.withColumn('language', when(col('language') == sp_utils.NA_VALUE, None).otherwise(col('language')))
    combined_df = akas_df.join(basics_df, akas_df.titleId == basics_df.tconst)

    title_type_language_count = combined_df.groupBy("titleType").agg(
        F.countDistinct("language").alias("distinct_languages"))

    title_type_max_languages = title_type_language_count.orderBy(F.desc("distinct_languages")).limit(1)

    sp_utils.save_dataframe(title_type_max_languages, "results_1_6")
    spark.stop()


if __name__ == "__main__":
    main()
