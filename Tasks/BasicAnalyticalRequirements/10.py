from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, col

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# director with most shows
aggDF = contentDF.groupBy('Director').agg(count('*').alias('ShowCount'))

# popular directors
maxShowCount = aggDF.agg(max('ShowCount').alias('MaxShowCount')).first()[0]
popularDirectors = aggDF.filter(col('ShowCount') == maxShowCount)
popularDirectors.show()

# save output as parquet
ouptut_path = './output/basic/popularDirector.parquet'
popularDirectors.write.mode('overwrite').parquet(ouptut_path)