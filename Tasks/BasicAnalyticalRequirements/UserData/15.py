from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, asc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# paired with Director and showcount
aggDF = contentDF.groupBy('Director').agg(count('*').alias('ShowCount'))

# max show and min show
maxDirectorDF = aggDF.orderBy(desc('ShowCount')).limit(1)
minDirectorDF = aggDF.orderBy(asc('ShowCount')).limit(1)

maxDirectorDF.show()
minDirectorDF.show()

# saving as parquet
output_path = './output/basic/'
maxDirectorDF.write.mode('overwrite').parquet(output_path+'directorWithMaxShow.parquet')
minDirectorDF.write.mode('overwrite').parquet(output_path+'directorWithMinShow.parquet')
