from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode, count, desc

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
popularActor = aggDF.orderBy(desc('ShowCount')).limit(1)
popularActor.show()

# save output as parquet
ouptut_path = './output/basic/popularActor.parquet'
popularActor.write.mode('overwrite').parquet(ouptut_path)