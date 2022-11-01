from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func

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

def createStartingRDD(data_path:str = ''):
    '''
    Reads the data source file and converts to RDD format
    '''
    inputFile = sc.textFile(data_path)
    return inputfile.map(graph_mapper)

def bfsMap(node):
    '''
    Receives single graph node and breaks into the data inside it 
    '''
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to expanded, it will be GRAY
    #The first needed to be expanded will always be the desired first character?
    if color == 'GRAY':
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if targetCharacterID == connection:
                hitCounter.add(1)

            newEntry = (newCharacterID ([], newDistance, newColor))
            results.append(newEntry)

        # This node was processed, so, give it a color BLACK
        color = 'BLACK'

    results.collect()

    results.append((characterID,(connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    '''
    Preserves darkest color and shortest path, comparing the nodes

    After a mapper in one iteration, the reduce comes to play

    This reduce function will receive two values (from the key value pair) and 
    check if the values correspond to the node with its original connections (with no empty connections)
    but the original ones, and add to a list of edges

    Also, preserve the minimum distance between the two values, since we are interested in the mininum distance to the 
    initial characterID

    Finally, preserve the darkest color among them, because the darkest color correponds to the last step of processed nodes, because
    we must remember that the whole list contains nodes which were processed and some which were not, that is the 'cleaning' process
    '''

    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[2]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # Check if some of them is the original node with the connections within
    # to preserve the connections
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance2):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color (most processed node)
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return_values = (edges, distance, color)

    #debug
    print(return_values)

    return return_values

if __name__ == '__main__':

    conf = SparkConf().setMaster("local").setAppName("BreadthFirstSearch")
    sc = SparkContext(conf = conf)

    # Characters we wish to find the degree of separation
    startCharacterID = 5306 #Spider man
    targetCharacterID = 14 #ADAM 3,3031 (who?)

    # Accumulator, used for triggering when we find the target character during traversal
    hitCounter = sc.accumulator(0)

    iterationRdd = createStartingRdd('data/marvel_graph_sample.txt')

    for iteration in range(0, 10):
        print('Running BFS iteration# ' + str(iteration +1))


        # Creates new vertices as needed, according to the mapper, for later
        # application of the reducer, which will select the darkest and shortest paths

        mapped = iterationRdd.flatmap(bfsMap)

        # The action of executing the method .count() from the rdd mapped will effectly apply the mapper
        print(f'Processing {str(mapped.count())} values')

        if (hitCounter.value > 0):
            print(f'Hitted the target character! From {str(hitCounter.value)} different direction(s)')
            break

        # Reducer applied to the mapped
        # Reducer By Key, that is why works with the way the bfsReduce receives only two value members, not keys
        iterationRdd = mapped.reduceByKey(bfsReduce)