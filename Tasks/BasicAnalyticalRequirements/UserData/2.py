from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, asc

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

uniqueDF = userDF.select('UserID', 'Subscription').distinct()
aggDF = userDF.groupBy('Subscription').agg(count('UserID').alias('SubscriberCount'))

# max users and min users
maxUserDF = aggDF.orderBy(desc('SubscriberCount')).limit(1)
minUserDF = aggDF.orderBy(asc('SubscriberCount')).limit(1)

maxUserDF.show()
minUserDF.show()

# saving as parquet
output_path = './output/basic/'
maxUserDF.write.mode('overwrite').parquet(output_path+'subscriptionwithmaxuser.parquet')
minUserDF.write.mode('overwrite').parquet(output_path+'subscriptionwithminuser.parquet')
