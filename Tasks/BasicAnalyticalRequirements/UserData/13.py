from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.getOrCreate()

# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

