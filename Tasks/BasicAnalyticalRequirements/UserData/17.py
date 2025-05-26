from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, to_date, datediff

spark = SparkSession.builder.getOrCreate()


# creating dataframe
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# column with diff
diffDF = engagementDF \
.withColumn('PlaybackStarted', to_date('PlaybackStarted', 'yyyy-MM-dd')) \
.withColumn('PlaybackStopped', to_date('PlaybackStopped', 'yyyy-MM-dd')) \
.withColumn('Duration', datediff('PlaybackStopped', 'PlaybackStarted'))

# grouped with userid
userDF = diffDF.groupBy('UserID').agg(sum('Duration').alias('DurationInDays'))
userDF.show()


# saving as parquet
output_path = './output/basic/userWithShowDuration.parquet'
userDF.write.mode('overwrite').parquet(output_path)
