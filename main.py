#2Sparks

from d3m import container
import datetime
from pathlib import Path
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import sys
import numpy as np
import re
from nltk.tokenize import word_tokenize
import nltk
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import format_string,col, when, coalesce, count, isnan,lit, udf
from pyspark.sql.types import StringType

from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important; }</style>"))


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

df2 = df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
    ) for c in df.columns])
    
df3 = spark.read.schema(df2.schema).parquet(*file_path_list)
df3.printSchema()

df4 = df3.dropDuplicates()
df4.count()

## Cleaning and Metrics of the dataset
labels=["a real animal","an animal body part"]
df4 = df3.filter(df3.label_product.isin(labels))
df4.limit(5)

df4.select('url').distinct().count()
df4.select('domain').distinct().count()

df4.groupBy(df4.seller)
df4.limit(5)

df4.select('location').distinct().count()
df4.groupBy("location").agg({"location":"count"}).show()

df4.groupBy("country").agg({"country":"count"}).show()

df4.summary("count").show() ## shows count of all the records wrt columns

df4.select([count(when(col(c).contains('None') | \
                            col(c).contains('null') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df4.columns]).show() ## shows count of non-null value records


##DS1
# csvFile = spark.read.format('csv').options(header='true',inferschema='true').load('datasets/01_taxa_use_combos.csv')
# csvFile.write.parquet("01_taxa_use_combos.csv.parquet")
parFile1 = spark.read.parquet('/user/ja4874_nyu_edu/01_taxa_use_combos.csv.parquet')
parFile1.printSchema()
parFile1.summary('count').show()
parFile1.show()

##DS2
# csvFile = spark.read.format('csv').options(header='true',inferschema='true').load('datasets/02_gbif_taxonomic_key.csv')
# csvFile.write.parquet("datasets/02_gbif_taxonomic_key.parquet")
parFile2 = spark.read.parquet('/user/ja4874_nyu_edu/datasets/02_gbif_taxonomic_key.parquet')
parFile2.printSchema()
parFile2.summary('count').show()
parFile2.show()

#DS3
# csvFile = spark.read.format('csv').options(header='true',inferschema='true').load('datasets/03_gbif_common_names.csv')
# csvFile.where(col('English name of language')=='English').drop('ISO 639-2 Code','ISO 639-1 Code','French name of Language','German name of Language','English name of Language').write.parquet("datasets/03_gbif_common_names.parquet")
parFile3 = spark.read.parquet('/user/ja4874_nyu_edu/datasets/03_gbif_common_names.parquet')
parFile3.printSchema()
parFile3.summary('count').show()
parFile3.show()

# Joining the datasets

parFile4=parFile2.join(parFile3, parFile2.id_species ==  parFile3.gbif_id)
#id_* can be removed
# count:15322
parFile5=parFile1.join(parFile4, parFile1.db_taxa_name ==  parFile4.db_taxa_name)
parFile5.show()

## Datasets Union (external) (FOR EXPERIMENTING)
cols=('species','subspecies','id_kingdom','id_phylum','id_class','id_order','id_family','id_genus','id_species','id_subspecies','match_type','gbif_name','gbif_rank','kingdom','phylum','class','order','family','genus')
parFile1.unionByName(parFile2, allowMissingColumns=True).drop(*cols).dropna().show(20)

parFile1.unionByName(parFile2, allowMissingColumns=True).drop(*cols).dropna().join(parFile3, 'gbif_id').join(parFile5, ['standardized_use_id','standardized_use_type']).dropna().show()

## Function to Extract the animal names from title
#animals = spark.read.format('csv').options(header='true',inferschema='true').load('/user/ja4874_nyu_edu/datasets/03_gbif_common_names.csv')
animalsArray = parFile3.select("gbif_common_name").toPandas().values.reshape(-1)
animalsArrayUnique = np.unique(animalsArray)
# animalsArrayUnique = list(map(lambda x: x.lower(), animalsArrayUnique))
print(animalsArrayUnique)


def extract_animals(title):
    matches = re.findall(fr'\b(?:{"|".join(animalsArrayUnique)})\b', title, flags=re.IGNORECASE)
    return matches[0] if matches else None

extract_animal_udf = udf(extract_animals, StringType())

##..... run only once and use the saved parquet file.....
df5 = df4.withColumn("animal_names", extract_animal_udf(col("title")))
df5.write.parquet("/user/ak10514_nyu_edu/animals.parquet")
## ........................................................
df6 = spark.read.parquet("/user/ak10514_nyu_edu/animals.parquet")
df6.limit(5)


## Remaining (Union the professor dataset and external dataset for analysis)
