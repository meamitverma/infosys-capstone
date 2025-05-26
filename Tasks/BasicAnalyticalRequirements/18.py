from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, to_date, datediff, desc

spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# column with diff
diffDF = engagementDF \
.withColumn('PlaybackStarted', to_date('PlaybackStarted', 'yyyy-MM-dd')) \
.withColumn('PlaybackStopped', to_date('PlaybackStopped', 'yyyy-MM-dd')) \
.withColumn('Duration', datediff('PlaybackStopped', 'PlaybackStarted'))

# user with longest playtime
userDF = diffDF.groupBy('UserID').agg(avg('Duration').alias('DurationInDays')).orderBy(desc('DurationInDays')).limit(1)
userDF.show()


# saving as parquet
output_path = './output/basic/userWithLongestPlaytime.parquet'
userDF.write.mode('overwrite').parquet(output_path)
