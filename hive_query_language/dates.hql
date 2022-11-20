-- days
select date_add(current_date, 2) as result

-- months
select add_months(current_date, 3) as result

-- general exaamples... 

-- https://sparksql.itversity.com/06_predefined_functions/05_date_manipulation_functions.html
select add_months(current_timestamp, 10) as result

select datediff('1992-07-27', '1993-07-07') as result

select date_sub('1993-07-07', 8) as result