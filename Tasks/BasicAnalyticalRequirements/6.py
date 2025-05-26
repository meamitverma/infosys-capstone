from pyspark.sql import SparkSession
from pyspark.sql.functions import count, asc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')


# earliest timestamp
earliestTimestampDF = userDF.orderBy(asc('Timestamp')).limit(1)
earliestTimestampDF.show()

# save output as parquet
ouptut_path = './output/basic/recordWithEarliestTimestamp.parquet'
earliestTimestampDF.write.mode('overwrite').parquet(ouptut_path)