-- https://sparksql.itversity.com/04_basic_ddl_and_dml/03_overview_of_data_types.html


CREATE TABLE students (
    student_id INT,
    student_first_name STRING,
    student_last_name STRING,
    student_phone_numbers ARRAY<STRING>,
    student_address STRUCT<street:STRING, city:STRING, state:STRING, zip:STRING>
) STORED AS TEXTFILE
ROW FORMAT
    DELIMITED FIELDS TERMINATED BY '\t'
    COLLECTION ITEMS TERMINATED BY ','

INSERT INTO students VALUES (1, 'Scott', 'Tiger', NULL, NULL)

INSERT INTO students VALUES (2, 'Donald', 'Duck', ARRAY('1234567890', '2345678901'), NULL)

INSERT INTO students VALUES 
    (3, 'Mickey', 'Mouse', ARRAY('1234567890', '2345678901'), STRUCT('A Street', 'One City', 'Some State', '12345')),
    (4, 'Bubble', 'Guppy', ARRAY('5678901234', '6789012345'), STRUCT('Bubbly Street', 'Guppy', 'La la land', '45678'))


-- SELECTS:

-- expand struct
 select student_address.street, student_address.city, student_address.state, student_address.zip from students;

-- expand array - lateral view expands the columns as rows
-- the view which as lateral is exploded into rows

select explode(student_phone_numbers) from students;


select student_id, phone_numbers from students lateral view explode(student_phone_numbers) phone_numbers_list as phone_numbers;

+----------+-------------+
|student_id|phone_numbers|
+----------+-------------+
|         3|   1234567890|
|         3|   2345678901|
|         2|          123|
|         2|          234|
|         4|   5678901234|
|         4|   6789012345|
+----------+-------------+