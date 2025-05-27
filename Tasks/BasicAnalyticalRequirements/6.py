from pyspark.sql import SparkSession
from pyspark.sql.functions import min

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')


# earliest timestamp
earliestTimestampDF = userDF.agg(min('Timestamp').alias('EarliestTimestamp')).limit(1)
earliestTimestampDF.show()

# save output as parquet
ouptut_path = './output/basic/recordWithEarliestTimestamp.parquet'
earliestTimestampDF.write.mode('overwrite').parquet(ouptut_path)