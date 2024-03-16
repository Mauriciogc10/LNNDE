"""
SQL test
could you please use these 2 tables and create the output
TABLE A
ID | VAL
10 | A
20 | BB
30 | CC


TABLE B
ID | VAL
20 | BB
30 | CC
40 | DD
10 | 


OUTPUT
ID | VAL | MSG
10 | A   | I BELONG TO TABLE A
40 | DD  | I BELONG TO TABLE B
30 | cc  | I inside two tables
"""

SELECT
    COALESCE(A.ID, B.ID) AS ID,
    COALESCE(UPPER(A.VAL), UPPER(B.VAL)) AS VAL,
    CASE
        WHEN A.ID IS NOT NULL AND B.ID IS NOT NULL THEN 'I inside two tables'
        WHEN A.ID IS NOT NULL THEN 'I BELONG TO TABLE A'
        WHEN B.ID IS NOT NULL THEN 'I BELONG TO TABLE B'
    END AS MSG
FROM
    TABLE_A A
FULL OUTER JOIN
    TABLE_B B
ON
    A.ID = B.ID;



Create Table Statement : 

========================


CREATE TABLE products (
 product_id INT PRIMARY KEY,
 product_name VARCHAR(50),
 category VARCHAR(50)
);


INSERT INTO products (product_id, product_name, category) VALUES
 (1, 'Laptops', 'Electronics'),
 (2, 'Jeans', 'Clothing'),
 (3, 'Chairs', 'Home Appliances');



CREATE TABLE sales (
 product_id INT,
 year INT,
 total_sales_revenue DECIMAL(10, 2),
 PRIMARY KEY (product_id, year),
 FOREIGN KEY (product_id) REFERENCES products(product_id)
);


INSERT INTO sales (product_id, year, total_sales_revenue) VALUES
 (1, 2019, 1000.00),
 (1, 2020, 1200.00),
 (1, 2021, 1100.00),
 (2, 2019, 500.00),
 (2, 2020, 600.00),
 (2, 2021, 900.00),
 (3, 2019, 300.00),
 (3, 2020, 450.00),
 (3, 2021, 400.00);


"""
Task : 
write a sql query to find the products whose total sales revenue has increased every year. Include the product_id , product_name and category in the result.

Table: Activity
+----------------+---------+
| Column Name | Type |
+----------------+---------+
| machine_id | int |
| process_id | int |
| activity_type | enum |
| timestamp | float |
+----------------+---------+
The table shows the user activities for a factory website.
(machine_id, process_id, activity_type) is the primary key (combination of columns with unique values) of this table.
machine_id is the ID of a machine.
process_id is the ID of a process running on the machine with ID machine_id.
activity_type is an ENUM (category) of type ('start', 'end').
timestamp is a float representing the current time in seconds.
'start' means the machine starts the process at the given timestamp and 'end' means the machine ends the process at the given timestamp.
The 'start' timestamp will always be before the 'end' timestamp for every (machine_id, process_id) pair.
 
There is a factory website that has several machines each running the same number of processes. Write a solution to find the average time each machine takes to complete a process.
The time to complete a process is the 'end' timestamp minus the 'start' timestamp. The average time is calculated by the total time to complete every process on the machine divided by the number of processes that were run.
The resulting table should have the machine_id along with the average time as processing_time, which should be rounded to 3 decimal places.
Return the result table in any order.
The result format is in the following example.
 
Example 1:
Input:
Activity table:
+------------+------------+---------------+-----------+
| machine_id | process_id | activity_type | timestamp |
+------------+------------+---------------+-----------+
| 0 | 0 | start | 0.712 |
| 0 | 0 | end | 1.520 |
| 0 | 1 | start | 3.140 |
| 0 | 1 | end | 4.120 |
| 1 | 0 | start | 0.550 |
| 1 | 0 | end | 1.550 |
| 1 | 1 | start | 0.430 |
| 1 | 1 | end | 1.420 |
| 2 | 0 | start | 4.100 |
| 2 | 0 | end | 4.512 |
| 2 | 1 | start | 2.500 |
| 2 | 1 | end | 5.000 |
+------------+------------+---------------+-----------+
Output:
+------------+-----------------+
| machine_id | processing_time |
+------------+-----------------+
| 0 | 0.894 |
| 1 | 0.995 |
| 2 | 1.456 |
+------------+-----------------+
Explanation:
There are 3 machines running 2 processes each.
Machine 0's average time is ((1.520 - 0.712) + (4.120 - 3.140)) / 2 = 0.894
"""
select distinct a.machine_id,round(avg(n.timestamp-a.timestamp),3) as processing_time 
from activity as a, activity as n
where a.machine_id = n.machine_id && a.process_id = n.process_id && a.activity_type = 'start' && n.activity_type = 'end'
Group by machine_id;

"""
#################################
𝐂𝐫𝐞𝐚𝐭𝐞 𝐓𝐚𝐛𝐥𝐞 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 :
=================
Create table If Not Exists Enrollments
(student_id int,
course_id int,
grade int
);

insert into Enrollments (student_id, course_id, grade) values ('2', '2', '95') ,
('2', '3', '95'),
('1', '1', '90'),
('1', '2', '99'),
('3', '1', '80'),
('3', '2', '75'),
('3', '3', '82');

𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 : Write a SQL query to find the highest grade with its corresponding course for each student. In case of a tie, you should find the course with the smallest course_id. The output must be sorted by increasing student_id.
"""
-- Method 1:

with cte as 
(
select student_id, course_id, grade, rank() over(partition by student_id order by grade desc, course_id) as rn
from enrollments
)
select student_id, course_id, grade
from cte where rn = 1;

-- Method 2:

with cte as 
(
select student_id, course_id, grade, dense_rank() over(partition by student_id order by grade desc, course_id) as rn
from enrollments
)
select student_id, course_id, grade
from cte where rn = 1;

𝐂𝐫𝐞𝐚𝐭𝐞 𝐓𝐚𝐛𝐥𝐞 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 :
=================
Create table If Not Exists Users
(
user_id int,
user_name varchar(20),
credit int
);

Create table If Not Exists Transactions
(
trans_id int,
paid_by int,
paid_to int,
amount int,
transacted_on date
);

insert into Users (user_id, user_name, credit) values
('1', 'Moustafa', '100'),
('2', 'Jonathan', '200'),
('3', 'Winston', '10000'),
('4', 'Luis', '800');

insert into Transactions (trans_id, paid_by, paid_to, amount, transacted_on) values
('1', '1', '3', '400', '2020-08-01'),
('2', '3', '2', '500', '2020-08-02'),
('3', '2', '1', '200', '2020-08-03');

select * from users;
select * from transactions;

𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 :
Write an SQL query to report.

user_iduser_name, credit, current balance after performing transactions.
credit_limit_breached, check credit_limit ("Yes" or "No")

WITH cte AS (
 SELECT 
 uu.user_id,
 uu.user_name,
 uu.credit,
 tt.amount,
 credit - COALESCE(amount, 0) AS Balance
 FROM 
 users AS uu
 LEFT JOIN 
 transactions AS tt ON uu.user_id = tt.paid_by
),
cte_2 AS (
 SELECT 
 uu.user_name,
 tt.paid_to,
 tt.amount as received
 FROM 
 users AS uu
 JOIN 
 transactions AS tt ON uu.user_id = tt.paid_to
),
Fnl as (
SELECT 
cte.user_id,
cte.user_name,
 balance + COALESCE(received,0) as credit
FROM
 cte
LEFT JOIN 
 cte_2 ON cte.user_id = cte_2.paid_to)
 
 Select 
 User_id,user_name,credit ,
 case when credit < 0 then 'Yes'
 else 'No' end as credit_limit_breached
 from Fnl



𝐂𝐫𝐞𝐚𝐭𝐞 𝐓𝐚𝐛𝐥𝐞 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 :

=================

Create table If Not Exists Failed (

fail_date date

);

Create table If Not Exists Succeeded (

success_date date

);

insert into Failed (fail_date)

values ('2018-12-28'),('2018-12-29'),('2019-01-04'),('2019-01-05');

insert into Succeeded (success_date) values ('2018-12-30'),('2018-12-31'),

('2019-01-01'),('2019-01-02'),('2019-01-03'),('2019-01-06');

𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 : Write an SQL query to generate a report of period_state for each continuous interval of days in the period from 𝟐𝟎𝟏𝟗-𝟎𝟏-𝟎𝟏 𝐭𝐨 𝟐𝟎𝟏𝟗-𝟏𝟐-𝟑𝟏.

WITH cte AS (
SELECT fail_date date, 'Failed' period_state FROM Failed
UNION ALL
SELECT success_date date, 'Succeeded ' period_state FROM Succeeded
) 
, cte_group as(
SELECT 
period_state,date,
CASE WHEN period_state != ISNULL( LAG(period_state) OVER(ORDER BY date) , '') THEN 1 ELSE NULL END grp
FROM cte 
WHERE year(date) = 2019
)
, cte_grouped AS (
select period_state
   ,date
   ,sum(grp) over(ORDER BY date) grp
   FROM cte_group
)
SELECT min(period_state), min(date) , max(date) 
FROM cte_grouped
GROUP BY grp

create table transactions (
        amount integer not null,
        tran_date date not null
  );

insert into transactions values ('1000', '2020-01-06');
insert into transactions values ('-10', '2020-01-14');
insert into transactions values ('-75', '2020-01-20');
insert into transactions values ('-5', '2020-01-25');
insert into transactions values ('-4', '2020-01-29');
insert into transactions values ('2000', '2020-03-10');
insert into transactions values ('-75', '2020-03-12');
insert into transactions values ('-20', '2020-03-15');
insert into transactions values ('40', '2020-03-15');
insert into transactions values ('-50', '2020-03-17');
insert into transactions values ('200', '2020-10-10');
insert into transactions values ('-200', '2020-10-10');

-- You are given a history of your bank account transactions for the year 2020. Each transactior

-- was either a credit card payment or an incoming transfer.

-- There is a fee for holding a credit card which you have to pay every month. The cost you arecharged each month is 5. However, you are not charged for a given month if you made atleast three credit card payments for a total cost of at least 100 within that month. Note that a

-- this fee is not included in the supplied history of transactions.At the beginning of the year, the balance of your account was 0. Your task is to compute the

-- balance at the end of the year.

-- You are given a table transactions with the following structure:

-- create table transactions

-- &mount integer not null,

-- date date not null

-- );

-- Each row of the table contains information about a single transaction: the amount of money

-- (amount) and the date when the transaction happened (date). If the amount value is negative,it is a credit card payment. Otherwise, it is an incoming transfer. There are no transactionswith an amount of 0..Write an SQL query that returns a table containing one column, balance. The table should

-- contain one row with the total balance of your account at the end of the year, including the

-- fee for holding a credit card.
-- The balance without the credit card fee would be 2801. You are charged a fee for everymonth except March, which in total equates to 11* 5 = 55.

-- In March, you had three transactions for a total cost of 75 + 20+ 50 = 145, thus you are notcharged the fee. In January, you had four card payments for a total cost of 10+ 75+ 5+4=94, which is less than 100; thus you are charged. In October, you had one card payment for atotal cost of 200 but you need to have at least three payments in a month; thus you arecharged. In all other months (February, April,...) you had no card payments, thus you arecharged.

-- The final balance is 2801 -55 = 2746.
WITH MonthlyBalance AS (
  SELECT
    EXTRACT(MONTH FROM tran_date) AS month,  -- Using date_trunc for clarity
    SUM(amount) AS total_amount,
    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS credit_card_payments,
    COUNT(CASE WHEN amount <= 0 THEN 1 END) AS card_payments_count,
    SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS credit_card_total
  FROM transactions
  GROUP BY month
),
MonthlyFee AS (
  SELECT
    month,
    CASE WHEN card_payments_count >= 3 AND credit_card_total >= 100 THEN 5 ELSE 0 END AS fee
  FROM MonthlyBalance
)
SELECT
  SUM(total_amount) - SUM(fee)-55 AS balance
FROM MonthlyBalance
JOIN MonthlyFee USING (MONTH)

------------------
------users with more than 10 min
create table phones (
      name varchar(20) not null unique,
      phone_number integer not null unique
  );

  create table calls (
      id integer not null,
      caller integer not null,
      callee integer not null,
      duration integer not null,
      unique(id)
  );
  
insert into phones values ('Jack', 1234);
insert into phones values ('Lena', 3333);
insert into phones values ('Mark', 9999);
insert into phones values ('Anna', 7582);
insert into calls values (25, 1234, 7582, 8);
insert into calls values (7, 9999, 7582, 1);
insert into calls values (18, 9999, 3333, 4);
insert into calls values (2, 7582, 3333, 3);
insert into calls values (3, 3333, 1234, 1);
insert into calls values (21, 3333, 1234, 1);

select * from phones
select * from calls

    select caller as phonenumber , sum(duration) as totaltime
    from calls
    group by caller
    
    select callee as phonenumber , sum(duration) as totaltime
    from calls
    group by callee
    
WITH TotalCallDuration AS (
    SELECT p.name, SUM(c.duration) AS total_duration
    FROM phones p
    JOIN (
        SELECT caller AS phone_number, duration
        FROM calls
        UNION ALL
        SELECT callee AS phone_number, duration
        FROM calls
    ) AS c ON p.phone_number = c.phone_number
    GROUP BY p.name
)
SELECT name
FROM TotalCallDuration
WHERE total_duration >= 10
ORDER BY name;