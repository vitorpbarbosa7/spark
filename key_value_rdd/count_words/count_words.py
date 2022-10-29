#Spark Boilerplate
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)


def show_rdd(rdd, custom_message:str = ''):
    print('\nRDD Printing:')
    print(custom_message)
    for item in rdd.collect():
        print(item)

book = sc.textFile('Book.txt')
words = book.flatMap(lambda word: word.split())

wordCount = words.countByValue()
print(wordCount)
wordCountSorted = sorted(wordCount.items(), key = lambda x: x[1], reverse = True)

