
-- Create Database
spark.sql('''create database itversity_retail''').show()

-- use
spark.sql(''' use itversity_retail''').show()

-- create table orders
spark.sql('''
CREATE TABLE orders (
    order_id INT,
    order_date STRING,
    order_customer_id INT,
    order_status STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',')
''').show()

-- load data
spark.sql('''
LOAD DATA LOCAL INPATH '/data/retail_db/orders' INTO TABLE orders
''').show()





-- Order Items
CREATE TABLE order_items (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) STORED AS parquet


-- Load data from local path 
LOAD DATA LOCAL INPATH '/data/retail_db/order_items'
    INTO TABLE order_items

-- THIS ABOVE WON'T WORK, SINCE THE ORIGINAL RAW DATA IS NOT IN PARQUET FORMAT, BUT IN PLAIN TEXT DELIMITED BY ','

--------------------------------------------------------------------------------------

-- HENCE, WE NEED TO USE stage
-- copying to local and after putting it into (using stage environment)

CREATE TABLE order_items_stage (
  order_item_id INT,
  order_item_order_id INT,
  order_item_product_id INT,
  order_item_quantity INT,
  order_item_subtotal FLOAT,
  order_item_product_price FLOAT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

LOAD DATA LOCAL INPATH '/data/retail_db/order_items' INTO TABLE order_items_stage

-- cleaning the previous loaded
TRUNCATE TABLE order_items

--finally   
-- INSERT INTO (appending data)
INSERT INTO TABLE order_items
SELECT * FROM order_items_stage


--OR 


-- INSERT OVERWRITE 
INSERT OVERWRITE TABLE order_items
SELECT * FROM order_items_stage




--------------------------------------
-- Putting all data from one table into only one column from another
CREATE EXTERNAL TABLE IF NOT EXISTS orders_single_column (
    s STRING
) LOCATION '/user/itversity/warehouse/itversity_retail.db/orders'

>>> spark.sql('''                                  
... select * from orders_single_column limit 10    
... ''').show()                                    
+--------------------+                             
|                   s|                             
+--------------------+                             
|1,2013-07-25 00:0...|                             
|2,2013-07-25 00:0...|                             
|3,2013-07-25 00:0...|                             
|4,2013-07-25 00:0...|                             
|5,2013-07-25 00:0...|                             
|6,2013-07-25 00:0...|                             
|7,2013-07-25 00:0...|                             
|8,2013-07-25 00:0...|                             
|9,2013-07-25 00:0...|                             
|10,2013-07-25 00:...|                             
+--------------------

-- select each from the split
spark.sql('''
SELECT split(s, ",")[0] AS order_id,
    split(s, ",")[1] AS order_date,
    split(s, ",")[2] AS order_customer_id,
    split(s, ",")[3] AS order_status
FROM orders_single_column LIMIT 10
''').show()

-- split using as type 
SELECT cast(split(s, ",")[0] AS INT) AS order_id,
    cast(split(s, ",")[1] AS TIMESTAMP) AS order_date,
    cast(split(s, ",")[2] AS INT) AS order_customer_id,
    cast(split(s, ",")[3] AS STRING) AS order_status
FROM orders_single_column LIMIT 10



--nvl2
-- if commission_pct is null, give it a value of 2 by default
SELECT s.*, 
    round(sales_amount * nvl2(commission_pct, commission_pct + 1, 2) / 100, 2) AS commission_amount
FROM sales AS s

-- simpler than using case when
select s.*
  case when commission_pct is NULL 
    then 2
    else commission_pct + 1
  end as commission_pct
from sales as s

-- invert
ELECT s.*, 
    CASE WHEN commission_pct IS NOT NULL 
        THEN commission_pct + 1
        ELSE 2
    END AS commission_pct
FROM sales AS s



-- word count explode
>>> spark.sql('''                                                                    
... select word, count(1) from (                                                     
... select explode(split(s, ' ')) as word from lines ) as q group by word''').show() 
+----------+--------+                                                                
|      word|count(1)|                                                                
+----------+--------+                                                                
|     World|       1|                                                                
|        us|       1|                                                                
|       you|       1|                                                                
|     count|       3|                                                                
|        is|       1|                                                                
|      each|       1|                                                                
|      data|       1|                                                                
|     Hello|       1|                                                                
|       the|       2|                                                                
|       How|       1|                                                                
|      word|       3|                                                                
|      from|       1|                                                                
|       are|       1|                                                                
|        of|       2|                                                                
|       The|       1|                                                                
|definition|       1|                                                                
|       get|       1|                                                                
|   perform|       1|                                                                
|      this|       1|                                                                
|        to|       1|                                                                
+----------+--------+                                                                
only showing top 20 rows

-- return number of unique words:
>>> spark.sql('''                                               
... SELECT count(1) FROM                                        
... (                                                           
...     SELECT word, count(1) FROM (                            
...         SELECT explode(split(s, ' ')) AS word FROM lines    
...     ) q                                                     
...     GROUP BY word                                           
... )                                                           
... ''').show()                                                 
+--------+                                                      
|count(1)|                                                      
+--------+                                                      
|      21|                                                      
+--------+                                                      
                                                                                                                           │itversity@itvdelab:~$ 
>>> 