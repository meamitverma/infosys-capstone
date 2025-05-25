from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# paired df of showid and count of users
aggDF = userDF.groupBy('ShowID').agg(count('UserID').alias('WatchCount'))

# showid with max number of count
showDF = aggDF.orderBy(desc('WatchCount')).limit(1)
showDF.show()

# save output as parquet
ouptut_path = './output/basic/showWithMaxWatchCount.parquet'
showDF.write.mode('overwrite').parquet(ouptut_path)