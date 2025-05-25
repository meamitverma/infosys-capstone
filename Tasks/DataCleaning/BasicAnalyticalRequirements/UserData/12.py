from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# number of shows per year
aggDF = contentDF.groupBy('Release_Year').agg(count('*').alias('ShowCount'))
aggDF.show()

# save output as parquet
ouptut_path = './output/basic/showsPerYear.parquet'
aggDF.write.mode('overwrite').parquet(ouptut_path)