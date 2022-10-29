#Spark Boilerplate
from pyspark import SparkConf, SparkContext
import collections 

def show_pair(rdd):
    '''
    Prints rdd key value pairs
    '''
    for key, value in rdd.collect():
        print(f'{key, value}')

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    '''
    Receives lines and return (key,value) of (age,numFriends) 
    '''

    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("fakefriends.csv")
rdd = lines.map(parseLine)

mapped_count = rdd.mapValues(lambda x: (x,1))
show_pair(mapped_count)

mean_values_by_key = mapped_count.reduceByKey(lambda x, y: (round((x[0] + y[0])/2,2), x[1] + y[1]))
mean_values_sorted = mean_values_by_key.sortByKey(True)
show_pair(mean_values_sorted)