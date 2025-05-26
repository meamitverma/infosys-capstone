from pyspark.sql import SparkSession
from pyspark.sql.functions import count,desc
spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# selecting only unique userid and location subscribed
filteredDF = userDF.select('UserID', 'Location').distinct()
locDF = filteredDF.groupBy('Location').agg(count('UserID').alias('SubscriberCount')).orderBy(desc('SubscriberCount')).limit(1)

# location with maximum number of users subscribed
locDF.show()

# saving as parquet
output_path = './output/basic/locwithmaxusers.parquet'
locDF.write.mode('overwrite').parquet(output_path)