#2Sparks
# PySpark Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("skectes").getOrCreate()

# Load your dataset into a DataFrame
file_path_list = ['/shared/CS-GY-6513/projects/WildLife/processed-data-oct/*.parquet']
data = spark.read.parquet(*file_path_list)

# Display basic summary statistics
# data_summary = data.summary()
data_summary = data.summary().toPandas()
print("Summary Statistics:")
print(data_summary)

# Compute approximate quantiles for numerical columns
numerical_cols = [col_name for col_name, data_type in data.dtypes if data_type in ['int', 'double']]
quantiles = data.approxQuantile(numerical_cols, [0.25, 0.5, 0.75], 0.01)
print("\nApproximate Quantiles:")
for i, colName in enumerate(numerical_cols):
    print(f"{colName}: {quantiles[i]}")

# Stop the Spark session
spark.stop()
