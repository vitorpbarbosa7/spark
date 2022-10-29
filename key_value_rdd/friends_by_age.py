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

# as beautiful simple dict
show_pair(rdd)

# Transformations:
rdd_by_age = rdd.reduceByKey(lambda x,y: (x+y)/2)
show_pair(rdd_by_age)
