#!/usr/bin/env python3

from pyspark.sql import SparkSession

# 1 buat spark session
spark = SparkSession.builder.appName("DataEngineering").getOrCreate()

# 2 baca dataset besar
csv_file_path = "/home/master/de_local/bigdata-processing-pyspark/yellow_tripdata_2015-01.csv"
df = spark.read.csv(f"file://{csv_file_path}", header=True, inferSchema=True)
df.show(5)

# 3 lakukan analisis sederhana
df.groupBy("payment_type").count().show()