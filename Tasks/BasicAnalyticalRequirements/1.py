from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, col
spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# selecting only unique userid and location subscribed
filteredDF = userDF.select('UserID', 'Location').distinct()
locDF = filteredDF.groupBy('Location').agg(count('UserID').alias('SubscriberCount'))

# location with maximum number of users subscribed
maxUsers = locDF.agg(max('SubscriberCount').alias('MaxCount')).first()[0]
locationsDF = locDF.filter(col('SubscriberCount') == maxUsers)
locationsDF.show()

# saving as parquet
output_path = './output/basic/locwithmaxusers.parquet'
locationsDF.write.mode('overwrite').parquet(output_path)