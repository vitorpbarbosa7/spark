#Spark Boilerplate
from pyspark import SparkConf, SparkContext
import collections 
from libs.helpers import show_pair

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def parseLine(line):
    '''
    Receives lines and return (key,value) of (age,numFriends) 
    '''

    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("friendsbyage.csv")
rdd = lines.map(parseLine)

mapped_count = rdd.mapValues(lambda x: (x,1))
show_pair(mapped_count)

mean_values_by_key = mapped_count.reduceByKey(lambda x, y: ((x[0] + y[0])/2, x[1] + y[1]))
show_pair(mean_values_by_key)
