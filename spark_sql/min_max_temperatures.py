from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("minTemperatures").getOrCreate()

schema = StructType(
    [
        StructField("station_id", StringType(), True),
        StructField("date", IntegerType(), True),
        StructField("measure_type", StringType(), True),
        StructField("temperature", FloatType(), True)
    ]
)

# File as dataframe
df = spark.read.schema(schema).csv('temperatures.csv')
df.printSchema()

# Min Temps
minTemps = df.filter(df.measure_type == "TMIN")
minTemps.show()

# Slice to show only some columns
stationTemps = minTemps.select('station_id','temperature')
stationTemps.show()

# Minimum temperature for every station
minTempByStation = (
    stationTemps
    .groupby('station_id')
    .agg(
        func.min('temperature')
        .alias('temperature')
        )
    )
minTempByStation.show()

# Sort dataset
sortedDataset = (
    minTempByStation
    .sort('temperature')
)
sortedDataset.show()

# Print results
for line in sortedDataset.collect():
        print(f'Station {line[0]} has a min temperature of {line[1]}')

spark.stop()