from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, col, min

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

uniqueDF = userDF.select('UserID', 'Subscription').distinct()
aggDF = userDF.groupBy('Subscription').agg(count('UserID').alias('SubscriberCount'))

# max users and min users
maxUsers = aggDF.agg(max('SubscriberCount').alias('MaxCount')).first()[0]
minUsers = aggDF.agg(min('SubscriberCount').alias('MinCount')).first()[0]

maxUserDF = aggDF.filter(col('SubscriberCount') == maxUsers)
minUserDF = aggDF.filter(col('SubscriberCount') == minUsers)

maxUserDF.show()
minUserDF.show()

# saving as parquet
output_path = './output/basic/'
maxUserDF.write.mode('overwrite').parquet(output_path+'subscriptionwithmaxuser.parquet')
minUserDF.write.mode('overwrite').parquet(output_path+'subscriptionwithminuser.parquet')
