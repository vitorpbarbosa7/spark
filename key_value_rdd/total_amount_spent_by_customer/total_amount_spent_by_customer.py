
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def show_rdd(rdd, custom_message:str = ''):
    print('\nRDD Printing:')
    print(custom_message)
    for item in rdd.collect():
        print(item)

def parseLines(line):
    fields = line.split(",")
    customer = fields[0]
    value = fields[2]

    return (customer, value)

rdd = sc.textFile('customer-orders.csv')
parsedLines = rdd.map(parseLines)
show_rdd(parsedLines)

# Total amount calculation, always in scalable manner
totalAmountByCustomer = parsedLines.reduceByKey(lambda x, y: round(float(x)+float(y),2)).sortBy(lambda line: line[1], ascending=True)
show_rdd(totalAmountByCustomer)
