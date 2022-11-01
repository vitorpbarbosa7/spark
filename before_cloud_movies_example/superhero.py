from tokenize import String, group
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

spark = SparkSession.builder.appName("SuperHero").getOrCreate()

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ]
)

names = spark.read.schema(schema).option("sep", " ").csv('data/marvel_names.txt')

lines = spark.read.text('data/marvel_graph.txt')
lines.show()

firstHero = lines.withColumn("id", func.split(func.col("value"), ' ')[0])
firstHero.show()

allConnections = firstHero.withColumn(
    'connections',func.size(func.split(func.col('value'),' '))-1)
allConnections.show()

groupedConnections = (
    allConnections
    .groupBy('id')
    .agg(
        func.sum(func.col('connections')).alias('sum_connections')
    )
    .orderBy(func.desc('sum_connections'))
)
groupedConnections.show()

mostPopularHeroID = groupedConnections.first()[0]
print(mostPopularHeroID)
mostPopularHeroConnectionNumber = groupedConnections.first()[1]

mostPopularHeroName = names.filter(names.id == mostPopularHeroID).first()[1]
print(f'The Most Popular Hero is {mostPopularHeroName} with {mostPopularHeroConnectionNumber} connections')

# Unpopular SuperHeroes:
minimunNumberConnections = groupedConnections.agg(func.min('sum_connections').alias('min')).first()[0]
print(minimunNumberConnections)

obscureSuperHeroesID = groupedConnections.filter(groupedConnections.sum_connections == minimunNumberConnections).select('id')

# Join
obscureSuperHeroesNames = obscureSuperHeroesID.join(
    names,
    on = 'id',
    how = 'left'
).select('name')
obscureSuperHeroesNames.show()

# Stop the session
spark.stop()
