from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, max

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# paired df of showid and count of users
aggDF = userDF.groupBy('ShowID').agg(count('UserID').alias('WatchCount'))

# showid with max number of count
maxCount = aggDF.agg(max('WatchCount').alias('MaxWatchCount')).first()[0]
showDF = aggDF.filter(col('WatchCount') == maxCount)
showDF.show()

# save output as parquet
output_path = './output/basic/showWithMaxWatchCount.parquet'
showDF.write.mode('overwrite').parquet(output_path)