#2Sparks
# PySpark Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("corr_skectes").getOrCreate()

# Load the first dataset
file_path_list = ['/shared/CS-GY-6513/projects/WildLife/processed-data-oct/*.parquet']
dataset1 = spark.read.parquet(*file_path_list)

# Load the second dataset
dataset2 = spark.read.csv('/ja4874_nyu_edu/datasets/01_taxa_name_gbif.csv', header=True, inferSchema=True)

# Display basic summary statistics for both datasets
print("Summary stats for DF 1:")
dataset1_summary = dataset1.summary().toPandas()
print(dataset1_summary)
print("\n")

print("Summary Stats for DF 2:")
dataset2_summary = dataset2.summary().toPandas()
print(dataset2_summary)

# Compute approximate quantiles for numerical columns in both datasets
numerical_cols1 = [col_name for col_name, data_type in dataset1.dtypes if data_type in ['int', 'double']]
quantiles1 = dataset1.approxQuantile(numerical_cols1, [0.25, 0.5, 0.75], 0.01)

numerical_cols2 = [col_name for col_name, data_type in dataset2.dtypes if data_type in ['int', 'double']]
quantiles2 = dataset2.approxQuantile(numerical_cols2, [0.25, 0.5, 0.75], 0.01)

print("\nApproximate Quantiles for DF 1:")
for i, col_name in enumerate(numerical_cols1):
    print(f"{col_name}: {quantiles1[i]}")
print("\n")
print("Approximate 1uantiles for DF 2:")
for i, col_name in enumerate(numerical_cols2):
    print(f"{col_name}: {quantiles2[i]}")

# Check the correlation between numerical columns in both datasets
correlation_matrix = dataset1.corr(dataset2, method="pearson")
print(correlation_matrix)

# Stop the Spark session
spark.stop()
