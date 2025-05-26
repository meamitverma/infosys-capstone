from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# counting unique genre
genreCountDF = contentDF.select('Genre').distinct().agg(count('*').alias('GenreCount'))

# unique genre count
genreCountDF.show()

# save output as parquet
ouptut_path = './output/basic/uniqueGenreCount.parquet'
genreCountDF.write.mode('overwrite').parquet(ouptut_path)