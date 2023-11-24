#2Sparks

from d3m import container
import datetime
from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import format_string,col, when, coalesce
cf = SparkConf()
cf.set("spark.submit.deployMode","client")
sc = SparkContext.getOrCreate(cf)
from pyspark.sql import SparkSession
spark = SparkSession \
	    .builder \
	    .config("spark.some.config.option", "some-value") \
	    .getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled",True)
sc.setLogLevel('OFF')


file_path_list = ['/shared/CS-GY-6513/projects/WildLife/processed-data-oct/*.parquet']
df = spark.read.parquet(*file_path_list)
#df =  pd.read_parquet('/home/ja4874_nyu_edu/processed-data-oct/crawl_data-1696285390462-0.parquet', engine='pyarrow')

for c in df.columns:
    df = df.withColumnRenamed(c, c.replace(" ", "_"))
    
df = spark.read.schema(df.schema).parquet(*file_path_list)
# df.write.csv('out1.csv')
print(df.count())
df.dropDuplicates()
print(df.count())


df.limit(20)

df = df.withColumn('title', when(coalesce('title', 'name', 'description', 'product').isNull(), 'PASS').otherwise('FAIL'))

# do not above line

df.select().where((df.description=="")).count()
