import os

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("test").getOrCreate()

# Specify the path to the TSV file
# tsv_file_path = "/opt/spark/data/data_example.tsv"
tsv_file_path = '/opt/data/data_example.tsv'
print(f'File path: {tsv_file_path}')

# Define the schema if needed
# schema = "col1 INT, col2 STRING, col3 DOUBLE"

# Read the TSV file into a DataFrame
# If you have a header in your TSV file, you can set header option to True
print('Reading dataframe')
df = spark.read.option("header","true") \
               .option("sep", "\t") \
               .option("multiLine", "true") \
               .option("quote","\"") \
               .option("escape","\"") \
               .option("ignoreTrailingWhiteSpace", True).csv(tsv_file_path)
print('Read successfully!')

# If you need to specify a custom schema, you can do so using the schema option
# df = spark.read.option("header", "true").option("delimiter", "\t").schema(schema).csv(tsv_file_path)

# Drop a column
print('Dropping column')
column_to_drop = 'a'
df_transformed = df.drop(column_to_drop)
print('Dropped')

# Save transformed data
print('Saving result')
output_tsv_path = "/opt/data/transformed"
df_transformed.write.option('header', True).csv(output_tsv_path)
print('Saved')

# Show the DataFrame
# df.show()

# Stop the Spark session
spark.stop()
