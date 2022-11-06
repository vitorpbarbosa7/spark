'''
UserID movie1 movie2 rating1 rating2
I A B 7 9 
I C D 8 6
II E C 5 9 
II F D 8 9
'''

import sys
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType

def computeCosineSimilarity(spark: SparkSession, data:SparkDataFrame) -> SparkDataFrame:
    '''
    Receives a SparkSession and a SparkDataFrame
    Computes the needed operations (Consine Similarity) for the SparkDataFrame
    and returns it with the modified features
    '''

    # applied within the ratings, so, using the pair of movies
    pairScores = (
        data
        .withColumn('xx', func.col('rating1') * func.col('rating1'))
        .withColumn('yy', func.col('rating2') * func.col('rating1'))
        .withColumn('xy', func.col('rating1') * func.col('rating2'))
    )

    '''
    From an array of ratings, calculate the similarity between the movies 
    in this structure, the movies only appear together within a same user
    But it appears together for more then on user, so, we get the cossine similarity 
    '''
    calculateSimilarity = (
        
        pairScores
        .groupBy('movie1', 'movie2')
        .agg(
            func.sum(func.col('xy')).alias('numerator'),
            (func.sqrt(func.sum(func.col('xx'))) * func.sqrt(func.sum(func.col('yy')))).alias('denominator'),
            func.count(func.col('xy')).alias('numPairs') 
        )
    )

    # case when with spark
    scoreCalculation = (
        calculateSimilarity
        .withColumn('score',
            func.when(func.col('denominator') != 0, func.col('numerator') / func.col('denominator')
            ).otherwise(0))
    )

    result = (
        scoreCalculation.select('movie1', 'movie2', 'score', 'numPairs')
    )

    return result

# script running only in local machine
spark = SparkSession.builder.appName('MovieSimilarity').master("local[*]").getOrCreate()

# construct schema instead of inferring them, since our data source requires this in that case
movieNameSchema = StructType([
    StructField('movieID', IntegerType(), True),
    StructField('movieTitle', StringType(), True)
])

# Broadcast dataset of movieID and movieTitle
movieNames = spark.read\
    .option(key = 'sep', value = '|')\
        .option(key = 'charset', value = 'ISO-8859-1')\
            .schema(movieNameSchema)\
                .csv(path = 'ml-100k/u.item')
print(movieNames.show())

def getMovieNames(movieNames:SparkDataFrame, movieID:int):
    '''
    Receives movieID and the movieNames and returns the movieName
    '''
    result = (
        movieNames.filter(
            (func.col('movieID') == movieID)
        ).select('movieTitle').collect()[0]
    )
    return result[0]

moviesSchema = StructType([
    StructField('userID', IntegerType(), True),
    StructField('movieID', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', IntegerType(), True)
])

# Load up the movies dataset
movies = spark.read\
    .option('sep', '\t')\
        .schema(moviesSchema)\
            .csv('ml-100k/u.data')

# ratings
ratings = movies.select('userID', 'movieID', 'rating')
# print(ratings.show())

# like a simple dataframe operation, with pandas or with sql, we can do a self join to get the pair of movies
df_self_join = (
    ratings
    .withColumnRenamed('movieID', 'movie1').withColumnRenamed('rating', 'rating1')
    .join(
        other = ratings.withColumnRenamed('movieID', 'movie2').withColumnRenamed('rating', 'rating2'),
        on = 'userID',
        how = 'inner',
    )
)
# print(df_self_join.show())
# print(df_self_join.count())

# filter, remove duplicates
# slicing as with pandas
df_remove_dup = (
    # removes the ones which are equal and the duplication
    df_self_join.filter(df_self_join['movie1'] < df_self_join['movie2'])
)
# print(df_remove_dup.count())
# print(df_remove_dup.show())

# caches the result of the operation in memory
df_cossine_similarity = computeCosineSimilarity(spark, df_remove_dup).cache()
print(df_cossine_similarity.show())

# extract from command line
if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOcurrenceThrehold = 50

    movieID = int(sys.argv[1])

    # which movies are similar to the movie from the movieID?
    # return the lines fro the dataframe which has this movieID
    # return only the ones with high score
    filteredMovie = (
        df_cossine_similarity
        .filter(
        ((func.col('movie1') == movieID) | (func.col('movie2') == movieID))
        )
        .filter(
            ((func.col('score') > scoreThreshold) & (func.col('numPairs')> coOcurrenceThrehold))
        )
    )

    orderedResults = filteredMovie.sort(func.col('score').desc()).take(10)

    # Display format for each row from the dataset Row (List of Row)
    for result in orderedResults:
        similarMovieID = result.movie1
        if result.movie1 == movieID:
            similarMovieID = result.movie2

        similarMovieName = getMovieNames(movieNames, similarMovieID)
        chosenMovieName = getMovieNames(movieNames, movieID)

        print(f'Movie {similarMovieName} is similar to {chosenMovieName} and the score is {result.score}')