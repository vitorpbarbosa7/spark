select 
current_date as current_date, 
current_timestamp as current_timestamp




select 
lower('HELLO WORLD') as lower_hello, 
upper('hello world') as upper_hello,
initcap('hello world') as initcap_hello,

length('hello world') as length_hello, 

substr('hello world', 4, 3) as substr_hello,

trim('    hello world   ') as trim_hello,
rtrim('    hello world   ') as rtrim_hello,
ltrim('    hello world   ') as ltrim_hello,

rpad('hello world', 20, ' ') as rpad_hello,
lpad('hello world', 20, ' ') as lpad_hello 




select 
split('2013-05-10', '-') as result_array

select 
split('2013-05-10', '-')[2] as result_0

select
split('2013-07-05', '-')[1] as result_only

-- explode to rows
select
explode(split('2015-05-12', '-')) as result_exploded




select reverse('musicadaxuxa') as reversed_string

select concat('hello','world') as concat_hello


--zfill ali no lpad
--padding, css box model, legal
select concat(year, '-', lpad(month, 2, 0), '-', lpad(myDate, 2, 0)) as order_date
from 
(
    select 2013 as year, 7 as month, 25 as myDate
) as runtime_table


-- concat_ws, concat with custom sep
select concat_ws('-', year, lpad(month, 2, 0), lpad(myDate, 2, 0)) as order_date
from 
(
    SELECT 2013 AS year, 7 AS month, 25 AS myDate
) as runtime_table