from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc

spark = SparkSession.builder.getOrCreate()

# creating dataframe
userDF = spark.read.parquet('./datasets/UserWatchData.parquet')
contentDF = spark.read.parquet('./datasets/ContentData.parquet')
engagementDF = spark.read.parquet('./datasets/EngagementData.parquet')

# filtered dataframes
filtered_user = userDF.select('UserID','Rating')
filtered_content = contentDF.select('ShowID','Genre')
filtered_engagement = engagementDF.select('UserID', 'ShowID')

# joined dataframe
userEngagementContent = filtered_user.join(filtered_engagement, on="UserID", how="inner")
userEngagementContent.show()

# joinedDF.show()

