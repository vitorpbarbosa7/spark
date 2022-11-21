# Scala examples adapted to pyspark 
# https://sparkbyexamples.com/spark/spark-add-hours-minutes-and-seconds-to-timestamp/


# different manners to create it
# https://stackoverflow.com/questions/57959759/manually-create-a-pyspark-dataframe

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

from pyspark.sql import Row


rdd = spark.sparkContext.parallelize([
    Row(input_timestamp="2019-01-15 05:04:02.222"),
    Row(input_timestamp="2019-06-24 12:01:19.222"),
    Row(input_timestamp="2019-11-16 16:44:55.406"),
    Row(input_timestamp="2019-11-16 16:50:59.406")
])

df = rdd.toDF()

df.createOrReplaceTempView("AddTimeExample")

spark.sql("select input_timestamp, " +
    "cast(input_timestamp as TIMESTAMP) + INTERVAL 2 hours as added_hours," +
    "cast(input_timestamp as TIMESTAMP) + INTERVAL 5 minutes as added_minutes," +
    "cast(input_timestamp as TIMESTAMP) + INTERVAL 55 seconds as added_seconds from AddTimeExample"
    ).show(100, False)
