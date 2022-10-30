from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text('book.txt')

# Splits using a regular expression to keep only words and not punctuation
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize to lower case
lowerCaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the ocurrences of each word in the book
wordCounts = lowerCaseWords.groupby("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort(func.col("count").desc())

# Show the results of operations
wordCountsSorted.show(wordCountsSorted.count())

