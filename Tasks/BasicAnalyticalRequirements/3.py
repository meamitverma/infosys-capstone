from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, col

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# paired df of showid and average rating
aggDF = userDF.groupBy('ShowID').agg(avg('Rating').alias('AverageRating'))
aggDF.show()

# shows with highest avg rating
maxRating = aggDF.agg(max('AverageRating').alias('MaxRating')).first()[0]
ratedShowDF = aggDF.filter(col('AverageRating') == maxRating)
ratedShowDF.show()

# save output as parquet
ouptut_path = './output/basic/showWithMaxAverageRating.parquet'
ratedShowDF.write.mode('overwrite').parquet(ouptut_path)