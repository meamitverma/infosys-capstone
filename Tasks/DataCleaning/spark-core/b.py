from pyspark.sql import SparkSession

# creating sparksession and sparkcontext
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# functions
def isValid(row, idxs):
    data = row.split(",")
    for idx in idxs:
        if (data[idx] == "?"): return False
    return True
        

def filterRDD(rdd, header, idxs=[0]):
    data = rdd.filter(lambda row : row != header) # remove header
    filtered_data = data.filter(lambda row : isValid(row, idxs))
    distinct_data = filtered_data.distinct()
    return distinct_data
    

# loading datasets from the hdfs into respective rdds
UserRDD = sc.textFile("./datasets/User_Data.csv")
ContentRDD = sc.textFile("./datasets/Content_Data.csv")
EngagementRDD = sc.textFile("./datasets/Engagement_Data.csv")

# headers
user_header = UserRDD.first()
content_header = ContentRDD.first()
engagement_header = EngagementRDD.first()

filtered_user = filterRDD(UserRDD, user_header)
filtered_content = filterRDD(ContentRDD, content_header)
filtered_engagement = filterRDD(EngagementRDD, engagement_header, [0, 1])

# fixing showids for userrdd after removing unwanted rows
filtered_user = filtered_user.map(lambda line : __import__('re').sub(r'\dM','M',line)).distinct()

print("-----UserData-----")
for data in filtered_user.collect():
    print(data)

# print("-----ContentData-----")
# for data in filtered_content.collect():
#     print(data)

# print("-----EngagementData-----")
# for data in filtered_engagement.collect():
#     print(data)