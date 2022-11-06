#https://docs.cloudera.com/runtime/7.2.6/developing-spark-applications/topics/spark-sql-example.html

from pyspark import SparkContext, SparkConf, HiveContext

if __name__ == "__main__":

  # create Spark context with Spark configuration
  conf = SparkConf().setAppName("Data Frame Join")
  sc = SparkContext(conf=conf)

  # What guarantees the connection to the environment where the database and datasets are
  sqlContext = HiveContext(sc)

  # From now on, any spark sql, spark rdd operations or SparkDataFrame code are allowed
  

  df_07 = sqlContext.sql("SELECT * from sample_07")
  df_07.filter(df_07.salary > 150000).show()
  df_08 = sqlContext.sql("SELECT * from sample_08")
  tbls = sqlContext.sql("show tables")
  tbls.show()
  df_09 = df_07.join(df_08, df_07.code == df_08.code).select(df_07.code,df_07.description)
  df_09.show()
  df_09.write.saveAsTable("sample_09")
  tbls = sqlContext.sql("show tables")
  tbls.show()