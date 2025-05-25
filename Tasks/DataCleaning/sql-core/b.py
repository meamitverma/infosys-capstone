from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()

# create dataframe
user_df = spark.read.csv('./datasets/UserRDD.csv', schema="UserID string,Age int,Location string,Subscription string,WatchHistory string",inferSchema=True, header=False)

# split
user_df_split = user_df.withColumn("WatchHistoryArray", split(col("WatchHistory"), '\|')).withColumn("Exploded", explode(col("WatchHistoryArray")) )
user_watch_df = user_df_split.withColumn("ShowID", split(col("Exploded"), ";").getItem(0)) \
                        .withColumn("Timestamp", split(col("Exploded"), ";").getItem(1)) \
                        .withColumn("Rating", split(col("Exploded"), ";").getItem(2)) \
                        .select("UserID", "Age", "Location", "Subscription", "ShowID", "Timestamp", "Rating")

user_watch_df.show()