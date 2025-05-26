from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# paired df of showid and average rating
aggDF = userDF.groupBy('ShowID').agg(avg('Rating').alias('AverageRating'))

# show with highest avg rating
ratedShowDF = aggDF.orderBy(desc('AverageRating')).limit(1)
ratedShowDF.show()

# save output as parquet
ouptut_path = './output/basic/showWithMaxAverageRating.parquet'
ratedShowDF.write.mode('overwrite').parquet(ouptut_path)