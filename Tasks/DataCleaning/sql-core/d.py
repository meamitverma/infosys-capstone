from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()

# create dataframe
content_df = spark.read.csv('./datasets/ContentRDD.csv', schema="ShowID string,Genre string,Actors string,Director string,Release_Year int,Synopsis string",inferSchema=True, header=False)

content_df.write.mode("overwrite").parquet('./datasets/ContentData.parquet')