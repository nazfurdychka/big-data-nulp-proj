from pyspark.sql import functions as F
import os
import sys
from pyspark.sql.functions import col, when

current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
parent_dir = os.path.dirname(current_dir)
proj_dir = os.path.dirname(parent_dir)
sys.path.extend([parent_dir, proj_dir])
import spark_apps.spark_utils as sp_utils


def main():
    spark = sp_utils.create_spark_session("Session_1_1")
    akas_df = sp_utils.read_data(spark, "title.akas.tsv")
    basics_df = sp_utils.read_data(spark, "title.basics.tsv")
    akas_df.withColumn('language', when(col('language') == sp_utils.NA_VALUE, None).otherwise(col('language')))

    movies_df = basics_df.filter(basics_df.titleType == 'movie')
    combined_df = akas_df.join(movies_df, akas_df.titleId == movies_df.tconst)

    language_count_df = combined_df.groupBy('tconst').agg(F.countDistinct('language').alias('language_count'))
    multilang_filtered_df = language_count_df.filter(language_count_df.language_count >= 2)

    movies_genres_df = multilang_filtered_df.join(movies_df, 'tconst')
    movies_genres_exploded = movies_genres_df.withColumn('genre', F.explode(F.split('genres', ',')))

    genre_counts = movies_genres_exploded.groupBy('genre').count()
    top_genres = genre_counts.orderBy('count', ascending=False).limit(5)

    sp_utils.save_dataframe(top_genres, "results_1_1")
    spark.stop()


if __name__ == "__main__":
    main()
