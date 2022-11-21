# Scala examples adapted to pyspark 
# https://sparkbyexamples.com/spark/spark-add-hours-minutes-and-seconds-to-timestamp/


# different manners to create it
# https://stackoverflow.com/questions/57959759/manually-create-a-pyspark-dataframe

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

from pyspark.sql import Row


spark.sql('''
select current_timestamp as current_timestamp,
date_format(current_timestamp, 'yyyy') as year
,date_format(current_timestamp, 'MM') as month
,date_format(current_timestamp, 'dd') as day
,date_format(current_timestamp, 'MMM') AS month_name_abvreviation
,date_format(current_timestamp, 'MMMM') AS month_name_full
,date_format(current_timestamp, 'EE') AS dayname_abvreviation
,date_format(current_timestamp, 'EEEE') AS dayname_full
''').show(20, False)

# +-----------------------+----+-----+---+-----------------------+---------------+--------------------+------------+
# |current_timestamp      |year|month|day|month_name_abvreviation|month_name_full|dayname_abvreviation|dayname_full|
# +-----------------------+----+-----+---+-----------------------+---------------+--------------------+------------+
# |2022-11-21 11:56:01.406|2022|11   |21 |Nov                    |November       |Mon                 |Monday      |
# +-----------------------+----+-----+---+-----------------------+---------------+--------------------+------------

spark.sql('''
select current_timestamp as current_timestamp
,date_format(current_timestamp, 'HH') AS hour24
,date_format(current_timestamp, 'hh') AS hour12
,date_format(current_timestamp, 'mm') AS minutes
,date_format(current_timestamp, 'ss') AS seconds
,date_format(current_timestamp, 'SS') AS millis
'''
).show(20, False)

# +-----------------------+------+------+-------+-------+------+
# |current_timestamp      |hour24|hour12|minutes|seconds|millis|
# +-----------------------+------+------+-------+-------+------+
# |2022-11-21 11:56:22.602|11    |11    |56     |22     |60    |
# +-----------------------+------+------+-------+-------+------


spark.sql('''
select current_timestamp as current_timestamp
,date_format(current_timestamp,'yyyyMM') as current_month
,date_format(current_timestamp,'yyyyMMdd') as current_date
,date_format(current_timestamp,'yyyy/MM/dd') as current_date_bar
''').show(20, False)

# +-----------------------+-------------+------------+----------------+
# |current_timestamp      |current_month|current_date|current_date_bar|
# +-----------------------+-------------+------------+----------------+
# |2022-11-21 11:58:18.519|202211       |20221121    |2022/11/21      |
# +-----------------------+-------------+------------+----------------+


spark.sql('''
select current_date as current_date_col
,year(current_date) AS year
,weekofyear(current_date) as weekofyear
,day(current_date) as da
,dayofmonth(current_date) as dayofmonth
''').show(20, False)

# +----------------+----+----------+---+----------+
# |current_date_col|year|weekofyear|da |dayofmonth|
# +----------------+----+----------+---+----------+
# |2022-11-21      |2022|47        |21 |21        |
# +----------------+----+----------+---+----------+


spark.sql('''
select unix_timestamp() as unixtime_function
,from_unixtime(1556662731) AS timestamp
,to_unix_timestamp('2022-11-21 12:07:00') as unixtime
,from_unixtime(1669043416, 'yyyyMM') as unix_to_yyyymm
,from_unixtime(1669043450, 'yyyy-MM-dd') as unix_to_yyyyMMdd
,from_unixtime(1669043510, 'yyyy-MM-dd HH:mm') as unix_to_yearmonthhouday
,from_unixtime(1669043600, 'yyyy-MM-dd HH:mm:ss.SS') as unix_to_full_timestamp
''').show(20, False)

# +-----------------+-------------------+----------+--------------+----------------+-----------------------+----------------------+
# |unixtime_function|timestamp          |unixtime  |unix_to_yyyymm|unix_to_yyyyMMdd|unix_to_yearmonthhouday|unix_to_full_timestamp|
# +-----------------+-------------------+----------+--------------+----------------+-----------------------+----------------------+
# |1669032860       |2019-04-30 22:18:51|1669032420|202211        |2022-11-21      |2022-11-21 15:11       |2022-11-21 15:13:20.00|
# +-----------------+-------------------+----------+--------------+----------------+-----------------------+----------------------+

spark.sql('''
select to_unix_timestamp('2019-04-30 18:18:51', 'yyyyMMdd') as date
,to_unix_timestamp('2019-04-30 18:18:51', 'yyyyMMdd HH:mm:ss') as timestamp
''').show(20, False)





















