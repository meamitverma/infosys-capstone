from pyspark.sql import SparkSession

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# loading datasets from the hdfs into respective rdds
UserRDD = sc.textFile("./Requirements/PySpark/User_Data.csv")
ContentRDD = sc.textFile("./Requirements/PySpark/Content_Data.csv")
EngagementRDD = sc.textFile("./Requirements/PySpark/Engagement_Data.csv")

# fetching five records excluding header from the rdds
user_five_records = UserRDD.take(6)
content_five_records = ContentRDD.take(6)
engagement_five_records = EngagementRDD.take(6)

# displaying the five fetched records
print("-----UserData-----")
for data in user_five_records:
    print(data)

print("-----ContentData-----")
for data in content_five_records:
    print(data)

print("-----EngagementData-----")
for data in engagement_five_records:
    print(data)


# calculating and printing total records in each rdd excluding header
user_total = UserRDD.count() - 1
content_total = ContentRDD.count() - 1
engagement_total = EngagementRDD.count() - 1

print("Total Records in UserRDD: ", user_total)
print("Total Records in ContentRDD: ", content_total)
print("Total Records in EngagementRDD: ", engagement_total)