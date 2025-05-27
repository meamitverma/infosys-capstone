from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode, count, max

spark = SparkSession.builder.getOrCreate()


# creating dataframe
contentDF = spark.read.parquet('./datasets/ContentData.parquet')

# splitted df by actor
splitDF = contentDF.withColumn('ActorsArray', split(col('Actors'),'\|')) \
    .withColumn('Actor', explode(col('ActorsArray')))

# filtered col
filteredDF = splitDF.select('Actor')

# count of show actor wise
aggDF = filteredDF.groupBy('Actor').agg(count('*').alias('ShowCount'))

# max show count
maxShowCount = aggDF.agg(max('ShowCount').alias('MaxShowCount')).first()[0]
popularActor = aggDF.filter(col('ShowCount') == maxShowCount)
popularActor.show()

# save output as parquet
ouptut_path = './output/basic/popularActor.parquet'
popularActor.write.mode('overwrite').parquet(ouptut_path)