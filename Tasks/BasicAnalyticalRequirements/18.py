from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, to_date, datediff, max, col

spark = SparkSession.builder.getOrCreate()

# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# column with diff
diffDF = engagementDF \
.withColumn('PlaybackStarted', to_date('PlaybackStarted', 'yyyy-MM-dd')) \
.withColumn('PlaybackStopped', to_date('PlaybackStopped', 'yyyy-MM-dd')) \
.withColumn('Duration', datediff('PlaybackStopped', 'PlaybackStarted'))

# user with longest playtime
aggDF = diffDF.groupBy('UserID').agg(avg('Duration').alias('DurationInDays'))
longestPlaytime = aggDF.agg(max('DurationInDays').alias('LongestPlaytime')).first()[0]
userDF = aggDF.filter(col('DurationInDays') == longestPlaytime)
userDF.show()


# saving as parquet
output_path = './output/basic/userWithLongestPlaytime.parquet'
userDF.write.mode('overwrite').parquet(output_path)
