import pyspark.sql.functions as F

import os
import sys

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])

import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_2_6")
    principals_data = sp_utils.read_data(spark, "title.principals.tsv")
    names_data = sp_utils.read_data(spark, "name.basics.tsv")
    professions_data = sp_utils.read_data(spark, "title.akas.tsv")

    principals_filtered = principals_data.filter(
        (principals_data["category"] == "actor") | (principals_data["category"] == "actress")
    )

    joined_data = principals_filtered\
        .join(names_data, "nconst")\
        .join(professions_data, principals_data["tconst"] == professions_data["titleId"])

    exploded_data = joined_data.select(
        "nconst", "primaryName", F.explode(F.split("primaryProfession", ",")).alias("profession")
    )

    unique_professions_count = exploded_data.groupBy("nconst", "primaryName").agg(
        F.countDistinct("profession").alias("unique_professions_count")
    )

    ranked_data = unique_professions_count.orderBy("unique_professions_count", ascending=False)

    sp_utils.save_dataframe(ranked_data, "results_2_6")
    spark.stop()


if __name__ == "__main__":
    main()
