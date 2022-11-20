from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ReadSchemaApp').master("local[*]").getOrCreate()

orders = spark.read.schema("order int, order_date string, order_customer_id int, order_status string").csv("/data/retail_db/orders")

orders.createOrReplaceTempView("orders_temp")

print(type(orders))

orders.show(10, False)