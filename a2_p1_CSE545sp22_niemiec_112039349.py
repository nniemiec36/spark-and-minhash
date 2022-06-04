# Name: Nicole Niemiec
# SBID: 112039349
# TASK I
# Sparkifying Income Descriptives


import sys
from pyspark import SparkContext
import math

input_file = sys.argv[1]
sc = SparkContext(appName="Homework 2")
# method quieted the output logs used from https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
logger = sc._jvm.org.apache.log4j
logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
income_rdd=sc.textFile(input_file, 32)


# GET MEDIAN
income_rdd = income_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).sortBy(lambda keyValue: keyValue[0])
index = math.floor(income_rdd.count() / 2)
medianList = income_rdd.collect()
(key, value) = medianList[index]
median = key

# GET TENS COUNT
tens = income_rdd.map(lambda word: (word, 1, str((10**math.floor(math.log(int(word[0]), 10)))))).map(lambda word: (word[2], 1)).reduceByKey(lambda a, b: a + b).collect()
# need to sort it outside of the RDD
tens = sorted(tens, key=lambda word: word[0])

# DISTINCT VALUES & MODE OF ALL INCOMES
counts = income_rdd.reduceByKey(lambda a, b: a + b)
find_mode = counts.sortBy(lambda keyValue: keyValue[1], ascending=False).collect()
(key, value) = find_mode[0]

print("")
print("Distinct # of values: ", counts.count())
print("Median of all incomes: ", median)
print("Mode of all incomes: ", key)
print("Count per power of 10: ", tens)
print("")

# trial ran in 30s
# test ran in 1.5min
# first transformable shuffle --> sortBy()
# second transformable shuffle --> reduceByKey()
# fourth transformable shuffle --> reduceByKey()
# third transformable shuffle --> sortBy()