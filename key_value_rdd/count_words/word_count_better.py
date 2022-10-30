
from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

def show_rdd(rdd, custom_message:str = ''):
    print('\nRDD Printing:')
    print(custom_message)
    for item in rdd.collect():
        print(item)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

book = sc.textFile('Book.txt')
words = book.flatMap(normalizeWords)
print(words)

# this way is not scalable, because the countByValue does not return an RDD
wordCount = words.countByValue()
print(wordCount)

wordCountSorted = sorted(wordCount.items(), key = lambda x: x[1], reverse = True)
# for key, value in wordCountSorted:
#     print(key, value)

# Scalable manner:
# Map creates the structure of key-value pair, for reducing by key and doing whatever you want
wordCountScalable = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
show_rdd(wordCountScalable)
# Tuples inside the RDD
sortedWordCountScalable = wordCountScalable.sortBy(lambda wordCountValue: wordCountValue[1], ascending = True)
show_rdd(sortedWordCountScalable)

# Flipping keys and values
'''
Found out why:
http://stackoverflow.com/questions/15712210/python-3-2-lambda-syntax-error
Use this code instead if you are using python 3+ :
wordCountsSorted = wordCounts.map(lambda xy: (xy[1],xy[0])).sortByKey()
'''
flippedWordCountScalable = wordCountScalable.map(lambda xy: (xy[1], xy[0]))
show_rdd(flippedWordCountScalable)

# Sorting by key after flipped
sortedFlippedWordCountScalable = flippedWordCountScalable.sortByKey()
show_rdd(sortedFlippedWordCountScalable)