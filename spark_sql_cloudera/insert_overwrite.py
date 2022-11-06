# https://kontext.tech/article/1067/spark-dynamic-and-static-partition-overwrite#:~:text=From%20version%202.3.,written%20into%20it%20at%20runtime.


# dynamic-overwrite-creation.py
from pyspark.sql import SparkSession

appName = "PySpark Example - Partition Dynamic Overwrite"
master = "local"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

data = [{
    'dt': '2022-01-01',
    'id': 1
}, {
    'dt': '2022-01-01',
    'id': 2
}, {
    'dt': '2022-01-01',
    'id': 3
}, {
    'dt': '2022-02-01',
    'id': 1
}]

df = spark.createDataFrame(data)
print(df.schema)
df.show()
df.repartition('dt').write.mode('overwrite').partitionBy('dt').format(
    'parquet').save('file:///home/kontext/pyspark-examples/data/parts')

# or 

# https://stackoverflow.com/questions/38487667/overwrite-specific-partitions-in-spark-dataframe-write-method
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
data.write.mode("overwrite").insertInto("partitioned_table"