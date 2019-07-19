from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# (33, 385), (33, 2)...
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///home/dmadhok/spark_course/fakefriends.csv")
rdd = lines.map(parseLine)

# (33, 385), (33, 2) => (33, (385, 1), (33, (2, 1))) => (33, (387, 2))
# (Age, (Total friends, Number of times that age was encountered))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# (33, (387 / 2))...
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
