from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# paired with genre and show count
aggDF = contentDF.groupBy('Genre').agg(count('Genre').alias('ShowCount'))

# show count with comedy genre
comedyCount = aggDF.where(col('Genre') == 'Comedy')
comedyCount.show()

# save output as parquet
ouptut_path = './output/basic/showWithComedyGenre.parquet'
comedyCount.write.mode('overwrite').parquet(ouptut_path)