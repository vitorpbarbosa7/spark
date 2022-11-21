-- days
select date_add(current_date, 2) as result

-- months
select add_months(current_date, 3) as result

-- general exaamples... 

-- https://sparksql.itversity.com/06_predefined_functions/05_date_manipulation_functions.html
select add_months(current_timestamp, 10) as result

select datediff('1992-07-27', '1993-07-07') as result

select date_sub('1993-07-07', 8) as result


-- Add hours
-- https://sparkbyexamples.com/spark/spark-add-hours-minutes-and-seconds-to-timestamp/
spark.sql('''
select current_timestamp,
cast(current_timestamp as timestamp) + INTERVAL 2 minutes as added_minutes,
-- BRAZILIAN TIME
cast(current_timestamp as timestamp) - INTERVAL 180 minutes as sub_hours,
cast(current_timestamp as timestamp) + INTERVAL 30 seconds as add_seconds
'''
).show(200, False)

+-----------------------+-----------------------+-----------------------+-----------------------+   
|current_timestamp()    |added_minutes          |sub_hours              |add_seconds            |   
+-----------------------+-----------------------+-----------------------+-----------------------+   
|2022-11-21 11:06:30.444|2022-11-21 11:08:30.444|2022-11-21 08:06:30.444|2022-11-21 11:07:00.444|   
+-----------------------+-----------------------+-----------------------+-----------------------+



df = Seq(())