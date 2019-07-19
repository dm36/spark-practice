from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# ITE00100554, 18000101, TMIN, -148 => (ITE00100554, TMIN, -234.4)...
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("file:///home/dmadhok/spark_course/1800.csv")
parsedLines = lines.map(parseLine)

# Only get the lines w/ TMIN as the second field
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# (ITE00100554, TMIN, -234.4) => (ITE00100554, -234.4)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
