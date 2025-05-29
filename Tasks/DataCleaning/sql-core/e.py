from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()

# create dataframe
engagement_df = spark.read.csv('./datasets/EngagementRDD.csv', schema="UserID string,ShowID string,PlaybackStarted string,PlaybackStopped string,CompletionPercent int",inferSchema=True, header=False)

engagement_df.write.mode("overwrite").parquet('./datasets/EngagementData.parquet')