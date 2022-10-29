#Spark Boilerplate
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def show_rdd(rdd, custom_message:str = ''):
    print('\nRDD Printing:')
    print(custom_message)
    for item in rdd.collect():
        print(item)

def parseLine(line):
    '''
    Receives lines and return a field's tuple
    '''

    fields = line.split(",")
    stationID = fields[0]
    temperature_label = fields[2]
    temperature = round(float(fields[3])*0.1*(9.0/5.0)+32,2)
    return (stationID, temperature_label, temperature)

file = sc.textFile('temperatures.csv')

parsedLines = file.map(parseLine)
show_rdd(parsedLines)

print(f'''
###################################
###################################
''')

minTemps = parsedLines.filter(lambda line: "TMIN" in line)
show_rdd(minTemps)

station_and_mintemp = minTemps.map(lambda line: (line[0], line[2]))
show_rdd(station_and_mintemp)

# Minimum temperature by station
min_temp_by_station = station_and_mintemp.reduceByKey(lambda x,y: min(x,y))
show_rdd(min_temp_by_station, 'minimum temperature')

min_temp_by_station = station_and_mintemp.reduceByKey(lambda x,y: max(x,y))
show_rdd(min_temp_by_station, 'maximum temperature')