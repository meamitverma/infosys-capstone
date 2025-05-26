from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# director with most shows
aggDF = contentDF.groupBy('Director').agg(count('*').alias('ShowCount'))
sortedDF = aggDF.orderBy(desc('ShowCount'))

# popular director
popularDirector = sortedDF.limit(1)
popularDirector.show()

# save output as parquet
ouptut_path = './output/basic/popularDirector.parquet'
popularDirector.write.mode('overwrite').parquet(ouptut_path)