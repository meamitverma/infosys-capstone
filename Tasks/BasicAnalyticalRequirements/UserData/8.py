from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.getOrCreate()


# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')

# users within age range of 18 to 30 inclusive
filteredUserDF = userDF.select('UserID', 'Age', 'Subscription').distinct().where(col('Age').between(18, 30))

# paired df of subscription and usercount
aggDF = filteredUserDF.groupBy('Subscription').agg(count('UserID').alias('UserCount'))
aggDF.show()

# save output as parquet
ouptut_path = './output/basic/usersBasedOnSubscriptionWithinRange.parquet'
aggDF.write.mode('overwrite').parquet(ouptut_path)