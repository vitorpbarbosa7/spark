
### Copy retail_db data from the container to the hdfs file system
```hql
hdfs dfs -put /data/retail_db /user/itversity
```

### create the database inside the hive
- Type "hive" at terminal and create the database
```hql
CREATE DATABASE retail_db;

USE retail_db;

CREATE EXTERNAL TABLE orders (
order_it INT, 
order_data STRING,
order_customer INT,
order_status STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/itversity/retail_db/orders';
```

- check if worked:
```hql
select count(*) from retail_db.orders;
```

### Export the PYSPARK_PYTHON env var
```bash
export PYSPARK_PYTHON=python3
```

### Create the pyspark3 alias at ~/.bashrc
```bash
alias pyspark3='/opt/spark3/bin/pyspark/'
```

### Test the spark by entering the pyspark REPL and typing:
```python
spark.sql('select count(*) from retail_db.orders').show()
```



