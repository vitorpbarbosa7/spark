from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

# With no header to infer, we create our own Structure
schema = StructType(
    [
        StructField("userID", StringType(), True),
        StructField("movieID", StringType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

# File as dataframe
df = spark.read.option("sep", "\t").schema(schema).csv('ml-100k/u.data')
df.printSchema()

# Find the movie with higher rating
sortedAvgRating = (
    df
    .groupby('movieID')
    .agg(
        func.round(func.avg('rating'),2)
        .alias('avg_rating')
    ). 
    sort(
        func.desc('avg_rating')
    )
)
sortedAvgRating.show()

# Find the most popular movie
sortedCount = (
    df
    .groupby('movieID')
    .count()
    .orderBy(
        func.desc('count')
        )
)
sortedCount.show()
