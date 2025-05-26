from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# unique and filtered column
uniqueDF = userDF.select('UserID', 'Location', 'Rating').distinct()

# paired df of location and average rating
aggDF = uniqueDF.groupBy('Location').agg(avg('Rating').alias('AverageRating'))

# location with highest avg rating
ratedLocationDF = aggDF.orderBy(desc('AverageRating')).limit(1)
ratedLocationDF.show()

# save output as parquet
ouptut_path = './output/basic/locationWithMaxAverageRating.parquet'
ratedLocationDF.write.mode('overwrite').parquet(ouptut_path)