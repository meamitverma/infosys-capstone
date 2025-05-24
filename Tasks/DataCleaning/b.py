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
        

def filter_and_sort(rdd, header, idxs=[0]):

    data = rdd.filter(lambda line : line != header) # removes header
    filtered_data = data.filter(lambda row : isValid(row, idxs)) # removes row with "?" 
    distinct_data = filtered_data.distinct() # remove duplicate records
    
    # sort the scattered data based on userid or showid
    sorted_data = distinct_data.sortBy(lambda data : data.split(",")[0]) 

    return sorted_data

def addHeader(raw_rdd, filtered_rdd):
    return sc.parallelize(raw_rdd.take(1)).union(filtered_rdd)

# loading datasets from the hdfs into respective rdds
UserRDD = sc.textFile("./Datasets/User_Data.csv")
ContentRDD = sc.textFile("./Datasets/Content_Data.csv")
EngagementRDD = sc.textFile("./Datasets/Engagement_Data.csv")

# headers
user_header = UserRDD.first()
content_header = ContentRDD.first()
engagement_header = EngagementRDD.first()

# filter rdds based on requirements
# indexes used for referring the userid,showid column number available in rdds
filtered_user = filter_and_sort(UserRDD, user_header)
filtered_content = filter_and_sort(ContentRDD, content_header)
filtered_engagement = filter_and_sort(EngagementRDD, engagement_header, [0, 1])

# add headers
user_data = addHeader(UserRDD,filtered_user)
content_data = addHeader(ContentRDD, filtered_content)
engagement_data = addHeader(EngagementRDD, filtered_engagement)

# display the final output
print("-----UserData-----")
for data in user_data.collect():
    print(data)

print("-----ContentData-----")
for data in content_data.collect():
    print(data)

print("-----EngagementData-----")
for data in engagement_data.collect():
    print(data)

