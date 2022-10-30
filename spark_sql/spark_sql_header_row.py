from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends_header.csv")

print("Infered Schema")
(
    people
    .printSchema()
)

print("Single Name Column")
(
    people
    .select("name")
    .show()
)

print("Filter feature likewise the boolean mask slicing with Pandas DataFrame")
(
    people
    .filter((people.age < 21))
    .show()
)

print("Group by age")
(
    people
    .groupBy("age")
    .count()
    .show()
)

print("Feature Engineering, age squared")
(
    people
    .select(people.name, people.age, people.age**2)
    .show()
)

print("Number of friends by Age")
(
    people
    .groupBy("age")
    .sum("number_of_friends")
    .withColumnRenamed("sum(number_of_friends)", "number_of_friends_by_age")
    .sort(func.col("number_of_friends_by_age"))
    .show()
)

print("Average number of friends by Age")
(
    people
    .groupBy("age")
    .agg(func.round(func.avg("number_of_friends"),2).alias("avg_number_of_friends_by_age"))
    .sort("avg_number_of_friends_by_age")
    .show()
)

spark.stop()
