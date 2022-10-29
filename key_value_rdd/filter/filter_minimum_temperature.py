#Spark Boilerplate
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def show_pair(rdd):
    '''
    Prints rdd key value pairs
    '''
    for key, value in rdd.collect():
        print(f'{key, value}')

def parseLine(line):
    '''
    Receives lines and return (key,value) of (age,numFriends) 
    '''

    fields = line.split(",")
    temperature_label = fields[2]
    temperature = int(fields[3])
    return (temperature_label, temperature)

file = sc.textFile('temperatures.csv')

parsedLines = file.map(parseLine)
show_pair(parsedLines)

print(f'''
###################################
###################################
''')

minTemps = parsedLines.filter(lambda line: "TMIN" in line)
show_pair(minTemps)