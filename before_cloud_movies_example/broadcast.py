from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    '''
    Creates dictionary of movies with 
    key - movieID
    value = movieName

    Ex:
    movieNames[key] = value
    {movieID, movieName}
    '''
    movieNames = {}
    with codecs.open("ml-100k/u.item",'r', encoding='ISO-8859-1', errors = 'ignore') as f:
        for line in f:
            fields = line.split('|')
            # key value pair
            # movieNames[key] = value
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Broadcast variable created from the application of a function on hard data, so is HARD DATA ?
movieNamesDicionary = spark.sparkContext.broadcast(loadMovieNames())

# With no header to infer, we create our own Structure
schema = StructType(
    [
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# File as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema).csv('ml-100k/u.data')
moviesDF.printSchema()

# Find the most popular movie
movieCounts = moviesDF.groupby('movieID').count()
movieCounts.show()

# look-up movie-names from the broadcasted dictionary
def lookupName(movieID):
    '''
    From the Mapper of movies and names, the broadcast variable movieNamesDicionary, 
    receives the movieID (key) and returns the value from that key, that is, the name (the movieNamesDicionary)
    '''
    return movieNamesDicionary.value[movieID]

# Creates the User Defined Function (udf)
lookupNameUDF = func.udf(lookupName)

# Adding the movie title column to the dataframe, in this parallelized, distribuited manner, which is less expensive then using joins
# since we know this is a simple mapper
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))
moviesWithNames.show()


