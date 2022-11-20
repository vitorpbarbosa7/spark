


- Create the external table
```sql
CREATE EXTERNAL TABLE orders (
  order_id INT COMMENT 'Unique order id',
  order_date STRING COMMENT 'Date on which order is placed',
  order_customer_id INT COMMENT 'Customer id who placed the order',
  order_status STRING COMMENT 'Current status of the order'
) COMMENT 'Table to save order level details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/itversity/external/retail_db/orders'
```

- Printing location
```python
>>> spark.sql('describe extended orders').select('data_type').show(20, False)  
```

+--------------------------------------------------------------+               
|data_type                                                     |               
+--------------------------------------------------------------+               
|int                                                           |               
|string                                                        |               
|int                                                           |               
|string                                                        |               
|                                                              |               
|                                                              |               
|itversity_retail                                              |               
|orders                                                        |               
|itversity                                                     |               
|Sat Nov 19 13:58:52 GMT 2022                                  |               
|UNKNOWN                                                       |               
|Spark 3.1.2                                                   |               
|EXTERNAL                                                      |               
|hive                                                          |               
|Table to save order level details                             |               
|[transient_lastDdlTime=1668866332]                            |               
|hdfs://localhost:9000/user/itversity/external/retail_db/orders|               
|org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe            |               
|org.apache.hadoop.mapred.TextInputFormat                      |               
|org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat    |               
+--------------------------------------------------------------+               
only showing top 20 rows


```python
>>> spark.sql('''                                                                                                                                           
... LOAD DATA LOCAL INPATH '/data/retail_db/orders'                                                                                                         
...   INTO TABLE orders                                                                                                                                     
... ''').show()                                                                                                                                             