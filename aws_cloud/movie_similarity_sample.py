import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
    movieNames = {}
    # with open("s3://vpb-spark-bucket/movies.dat",  encoding='ascii', errors='ignore') as file:
    with open("movies.dat",  encoding='ascii', errors='ignore') as file:
        for line in file:
            fields = line.split('::')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(userRatings):
    '''
    Change format of (userid ((m1, r1), (m2, r2))) to ((m1, m2), (r1,r2))
    that is, the user is not necessary anymore, since we have the movie pairs to group by and calculate the 
    cosine similarity afterwards
    '''
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates(userRatings):
    '''
    Extracts the structure of the rdd and compares m1 and m2
    '''
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingsPairs):
    '''
    Using map, will apply this operation for each key in the rdd
    '''
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingsPairs:
        sum_xx += ratingX*ratingX
        sum_yy += ratingY*ratingY
        sum_xy += ratingX*ratingY
        numPairs += 1
    
    numerator = sum_xy
    denominator = sqrt(sum_xx)*sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

conf = SparkConf()
sc = SparkContext(conf = conf)

print(f'\n Loading movie names')
nameDict = loadMovieNames()

# data = sc.textFile('s3://vpb-spark-bucket/ratings_sample.dat')
data = sc.textFile('ratings_sample.dat')

#for each row of ratings.dat, map it according to key value
ratings = data.map(lambda l: l.split('::')).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))

# once the key is created
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

# Now we have the key UserID, and the pair ((movieID, rating),(movieId, rating))

#filter out duplicated pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

#change format
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

# with the pair, we can group by key now to compute similarity
moviePairRatings = moviePairs.groupByKey()

moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()

# Save the results if desires
moviePairSimilarities.sortByKey()
moviePairSimilarities.saveAsTextFile('movie_sims')

# Extract similarities for the movie we care about that are good
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))