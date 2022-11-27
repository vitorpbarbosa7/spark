-- returns one row for each group from the group by clause
select 
department_id,
count(employee_id) as count_employees 
from 
employees
group by 
department_id

-- It does not return only a single row per group, by returns all rows, with grouped result 
select
department_id,
count(1) over (partition by department_id) as employee_count
from employees

-- This way , we can use a order by clause 
select
department_id,
count(1) over (partition by department_id) as employee_count
from employees
order by employee_count desc

-- with salary in 
select
department_id, salary,
count(1) over (partition by department_id) as employee_count
from employees
order by employee_count desc


-- classic use rank() with partition by and order by
select 
employee_id, department_id, salary
from 
(
    select 
    employee_id, department_id, salary, 
    rank() over (partition by department_id order by salary desc) as rnk
    from 
    employees
)
where rnk = 1

--LEAD: return next value from the table
select
department_id, salary, employee_id,
lag(employee_id, 1) over (partition by department_id order by salary desc) as lag_emp_id_1,
lead(employee_id) over (partition by department_id order by salary desc) as lead_emp_id,
lead(employee_id, 2) over (partition by department_id order by salary desc) as lead_emp_id_2,
lead(employee_id, 3) over (partition by department_id order by salary desc) as lead_emp_id_3
from employees
order by employee_id

+-------------+--------+-----------+------------+-----------+-------------+-------------+
|department_id|  salary|employee_id|lag_emp_id_1|lead_emp_id|lead_emp_id_2|lead_emp_id_3|
+-------------+--------+-----------+------------+-----------+-------------+-------------+
|           90|24000.00|        100|        null|        101|          102|         null|
|           90|17000.00|        101|         100|        102|         null|         null|
|           90|17000.00|        102|         101|       null|         null|         null|
|           60| 9000.00|        103|        null|        104|          105|          106|
|           60| 6000.00|        104|         103|        105|          106|          107|
|           60| 4800.00|        105|         104|        106|          107|         null|
|           60| 4800.00|        106|         105|        107|         null|         null|
|           60| 4200.00|        107|         106|       null|         null|         null|
|          100|12000.00|        108|        null|        109|          110|          112|
|          100| 9000.00|        109|         108|        110|          112|          111|
|          100| 8200.00|        110|         109|        112|          111|          113|
|          100| 7700.00|        111|         112|        113|         null|         null|
|          100| 7800.00|        112|         110|        111|          113|         null|
|          100| 6900.00|        113|         111|       null|         null|         null|
|           30|11000.00|        114|        null|        115|          116|          117|
|           30| 3100.00|        115|         114|        116|          117|          118|
|           30| 2900.00|        116|         115|        117|          118|          119|
|           30| 2800.00|        117|         116|        118|          119|         null|
|           30| 2600.00|        118|         117|        119|         null|         null|
|           30| 2500.00|        119|         118|       null|         null|         null|
+-------------+--------+-----------+------------+-----------+-------------+-------------+




-- USING TOGETHER
select 
employee_id, department_id, salary,
    count(1) over (partition by department_id) as employee_count,
    rank() over (order by salary desc) as rnk,
    lead(employee_id) over (partition by department_id order by salary desc) as lead_emp_id,
    lead(salary) over (partition by department_id order by salary desc) as lead_emp_salary
from employees
order by employee_id

+-----------+-------------+--------+--------------+---+-----------+---------------+
|employee_id|department_id|  salary|employee_count|rnk|lead_emp_id|lead_emp_salary|
+-----------+-------------+--------+--------------+---+-----------+---------------+
|        100|           90|24000.00|             3|  1|        101|       17000.00|
|        101|           90|17000.00|             3|  2|        102|       17000.00|
|        102|           90|17000.00|             3|  2|       null|           null|
|        103|           60| 9000.00|             5| 24|        104|        6000.00|
|        104|           60| 6000.00|             5| 56|        105|        4800.00|
|        105|           60| 4800.00|             5| 59|        106|        4800.00|
|        106|           60| 4800.00|             5| 59|        107|        4200.00|
|        107|           60| 4200.00|             5| 62|       null|           null|
|        108|          100|12000.00|             6|  7|        109|        9000.00|
|        109|          100| 9000.00|             6| 24|        110|        8200.00|
|        110|          100| 8200.00|             6| 32|        112|        7800.00|
|        111|          100| 7700.00|             6| 39|        113|        6900.00|
|        112|          100| 7800.00|             6| 38|        111|        7700.00|
|        113|          100| 6900.00|             6| 48|       null|           null|
|        114|           30|11000.00|             6| 11|        115|        3100.00|
|        115|           30| 3100.00|             6| 78|        116|        2900.00|
|        116|           30| 2900.00|             6| 84|        117|        2800.00|
|        117|           30| 2800.00|             6| 87|        118|        2600.00|
|        118|           30| 2600.00|             6| 93|        119|        2500.00|
|        119|           30| 2500.00|             6| 97|       null|           null|
+-----------+-------------+--------+--------------+---+-----------+---------------+


-- example
doc	mes	value 			
1	202201	984			LEAD(VALUE, 1) OVER (PARTITIONED BY DOC ORDER BY MES) AS LAG_1_VALUE
2	202202	654			
3	202203	654			
4	202204	54			
					
					
1	202201	6545			
2	202202	4987			
3	202203	6544			
4	202204	6544			


-- group by cluase example, which requires to make joins with the original table and the summarized table
SELECT e.employee_id, e.department_id, e.salary,
       ae.department_salary_expense,
       ae.avg_salary_expense
FROM employees e JOIN (
     SELECT department_id, 
            sum(salary) AS department_salary_expense,
            avg(salary) AS avg_salary_expense
     FROM employees
     GROUP BY department_id
) ae
ON e.department_id = ae.department_id
ORDER BY department_id, salary

+-----------+-------------+--------+-------------------------+------------------+
|employee_id|department_id|  salary|department_salary_expense|avg_salary_expense|
+-----------+-------------+--------+-------------------------+------------------+
|        200|           10| 4400.00|                  4400.00|       4400.000000|
|        202|           20| 6000.00|                 19000.00|       9500.000000|
|        201|           20|13000.00|                 19000.00|       9500.000000|
|        119|           30| 2500.00|                 24900.00|       4150.000000|
|        118|           30| 2600.00|                 24900.00|       4150.000000|
|        117|           30| 2800.00|                 24900.00|       4150.000000|
|        116|           30| 2900.00|                 24900.00|       4150.000000|
|        115|           30| 3100.00|                 24900.00|       4150.000000|
|        114|           30|11000.00|                 24900.00|       4150.000000|
|        203|           40| 6500.00|                  6500.00|       6500.000000|
|        132|           50| 2100.00|                156400.00|       3475.555556|
|        128|           50| 2200.00|                156400.00|       3475.555556|
|        136|           50| 2200.00|                156400.00|       3475.555556|
|        127|           50| 2400.00|                156400.00|       3475.555556|
|        135|           50| 2400.00|                156400.00|       3475.555556|
|        144|           50| 2500.00|                156400.00|       3475.555556|
|        182|           50| 2500.00|                156400.00|       3475.555556|
|        131|           50| 2500.00|                156400.00|       3475.555556|
|        191|           50| 2500.00|                156400.00|       3475.555556|
|        140|           50| 2500.00|                156400.00|       3475.555556|
+-----------+-------------+--------+-------------------------+------------------+



-- with windowing functions
select 
    employee_id, department_id, salary,
    sum(salary) over (partition by department_id) as department_salary_expense,
    avg(salary) over (partition by department_id) as avg_salary_expense
from employees
order by department_id, salary

+-----------+-------------+--------+-------------------------+------------------+
|employee_id|department_id|  salary|department_salary_expense|avg_salary_expense|
+-----------+-------------+--------+-------------------------+------------------+
|        178|         null| 7000.00|                  7000.00|       7000.000000|
|        200|           10| 4400.00|                  4400.00|       4400.000000|
|        202|           20| 6000.00|                 19000.00|       9500.000000|
|        201|           20|13000.00|                 19000.00|       9500.000000|
|        119|           30| 2500.00|                 24900.00|       4150.000000|
|        118|           30| 2600.00|                 24900.00|       4150.000000|
|        117|           30| 2800.00|                 24900.00|       4150.000000|
|        116|           30| 2900.00|                 24900.00|       4150.000000|
|        115|           30| 3100.00|                 24900.00|       4150.000000|
|        114|           30|11000.00|                 24900.00|       4150.000000|
|        203|           40| 6500.00|                  6500.00|       6500.000000|
|        132|           50| 2100.00|                156400.00|       3475.555556|
|        128|           50| 2200.00|                156400.00|       3475.555556|
|        136|           50| 2200.00|                156400.00|       3475.555556|
|        127|           50| 2400.00|                156400.00|       3475.555556|
|        135|           50| 2400.00|                156400.00|       3475.555556|
|        131|           50| 2500.00|                156400.00|       3475.555556|
|        182|           50| 2500.00|                156400.00|       3475.555556|
|        191|           50| 2500.00|                156400.00|       3475.555556|
|        144|           50| 2500.00|                156400.00|       3475.555556|
+-----------+-------------+--------+-------------------------+------------------+


spark.sql("""
    SELECT t.*,
      LEAD(order_item_product_id) OVER (
        PARTITION BY order_date 
        ORDER BY revenue DESC
      ) next_product_id,
      LEAD(revenue) OVER (
        PARTITION BY order_date 
        ORDER BY revenue DESC
      ) next_revenue
    FROM daily_product_revenue t
    ORDER BY order_date, revenue DESC
""").show(100, False)



-- FIRST AND LAST VALUE
select 
t.*, 
first_value(order_item_product_id) over (partition by order_date order by revenue desc) first_product_id
from daily_product_revenue as t
order by order_date, revenue desc
limit 100