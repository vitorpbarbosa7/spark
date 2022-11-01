from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

conf = SparkConf().setMaster("local").setAppName("BreadthFirstSearch")
sc = SparkContext(conf = conf)

def graph_mapper(line):
    '''
    UDF to map the lines to the format of Tuple Representing the Degrees of Separation
    '''

    distance = 9999
    color = 'WHITE'

    fields = line.split(' ')

    heroID = int(fields[0])
    connections = []
    for connection in fields[1:-1]:
        connections.append(int(connection))

    if heroID == startCharacterID:
        distance = 0
        color = 'GRAY'

    return (heroID, (connections, distance, color))

lines = sc.textFile('data/marvel_graph.txt')
graph = lines.map(graph_mapper)

for key, value in graph.collect():
    print(key, value)




    