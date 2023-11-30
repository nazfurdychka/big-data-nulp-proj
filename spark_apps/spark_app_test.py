from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create a Spark session
spark = SparkSession.builder.appName("GenerateAndWriteDataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

# Generate a large DataFrame with random data
num_rows = 1000000  # Adjust the number of rows as needed
data = [(i, f"Name{i}", i % 100) for i in range(num_rows)]
df = spark.createDataFrame(data, schema=schema)

# Show a sample of the DataFrame
df.show(5)

# Write the DataFrame to a Parquet file (you can choose a different format if needed)
output_path = "/opt/data/test_data"
df.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
