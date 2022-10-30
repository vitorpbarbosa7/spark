from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Instead of Spark Context, for Spark SQL we create a Spark Session
# It's like opening a database session, so we must close it at the end
spark = SparkSession.builder.appName("CustomerOrders").getOrCreate()

schema = StructType(
    [
        StructField("customer", StringType(), True),
        StructField("product", StringType(), True),
        StructField("value", FloatType(), True)
    ]
)

# File as dataframe
df = spark.read.schema(schema).csv('customer-orders.csv')
df.printSchema()

# Total spent by customer
totalSpentByCustomer = (
    df
    .groupby('customer')
    .agg(
        func.round(func.sum('value'),2)
        .alias('total_spent_by_customer')
    )
    .sort('total_spent_by_customer')
)
totalSpentByCustomer.show()

spark.stop()