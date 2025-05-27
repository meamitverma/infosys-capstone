from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, max, min

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# paired with Director and showcount
aggDF = contentDF.groupBy('Director').agg(count('*').alias('ShowCount'))

# max show and min show
maxShowCount = aggDF.agg(max('ShowCount').alias('MaxShowCount')).first()[0]
minShowCount = aggDF.agg(min('ShowCount').alias('MinShowCount')).first()[0]

maxDirectorDF = aggDF.filter(col('ShowCount') == maxShowCount)
minDirectorDF = aggDF.filter(col('ShowCount') == minShowCount)

maxDirectorDF.show()
minDirectorDF.show()

# saving as parquet
output_path = './output/basic/'
maxDirectorDF.write.mode('overwrite').parquet(output_path+'directorWithMaxShow.parquet')
minDirectorDF.write.mode('overwrite').parquet(output_path+'directorWithMinShow.parquet')
