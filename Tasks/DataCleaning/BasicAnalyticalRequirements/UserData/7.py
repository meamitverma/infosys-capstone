from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# paired df of showid and count of ratings received
aggDF = userDF.groupBy('ShowID').agg(count('Rating').alias('RatingCount'))
aggDF.show()

# save output as parquet
ouptut_path = './output/basic/showWitRatingCount.parquet'
aggDF.write.mode('overwrite').parquet(ouptut_path)