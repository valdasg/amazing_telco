-- Active: 1670224489683@@127.0.0.1@5432@postgres@public

-- Utilities
TRUNCATE TABLE dim_priceplans;
DROP TABLE dim_priceplans;

TRUNCATE TABLE fct_subscriptions;
DROP TABLE fct_subscriptions;

-- creating a priceplan table, assuming price plan, to explore data
CREATE TABLE dim_priceplans(  
    soc_pp_code VARCHAR(255),
    product_group VARCHAR(255),
    brand VARCHAR(255),
    product_segment VARCHAR(255),
    product_payment_type VARCHAR(255)
);

-- importing priceplan csv file
COPY dim_priceplans (
    soc_pp_code,
    product_group,
    brand,
    product_segment,
    product_payment_type)
FROM '/Users/valdasgylys/Downloads/priceplan_hierarchy_anonymized.csv'
DELIMITER ','
CSV HEADER;

-- checking data
SELECT *
FROM dim_priceplans
LIMIT 10;

-- assuming soc_pp_code could be set as primary key to dimension table, checking if soc_pp_code is unique
SELECT DISTINCT(ROW_NUMBER() OVER(PARTITION BY soc_pp_code))
FROM dim_priceplans;

-- setting up constraints and primary key
ALTER TABLE dim_priceplans
ADD CONSTRAINT PP_ID UNIQUE(soc_pp_code);

ALTER TABLE dim_priceplans
ALTER COLUMN soc_pp_code SET NOT NULL;

ALTER TABLE dim_priceplans
ADD PRIMARY KEY (soc_pp_code);

-- creating subscrpiptions fact table and primary key as well as foreign key referencing to dimensions table
CREATE TABLE fct_subscriptions(
    transaction_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    subscriber_no VARCHAR(255),
    ban VARCHAR(255),
    subscriber_id INT,
    effective_date TIMESTAMP,
    expiration_date TIMESTAMP,
    FOREIGN KEY(soc_pp_code) 
    REFERENCES dim_priceplans(soc_pp_code)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
    soc_pp_code VARCHAR(255),
    rate DECIMAL(12,2)
);


-- importing subscription csv file
COPY fct_subscriptions (subscriber_no, ban, subscriber_id, effective_date, expiration_date, soc_pp_code, rate)
FROM '/Users/valdasgylys/Downloads/subscriptions_history_sample_anonymized.csv'
DELIMITER ','
CSV HEADER;

-- check the data
SELECT *
FROM fct_subscriptions;
---------------------------------------------------------------------------

-- QUESTION 1: How many plans do we have?

-- Count the plans from dim_piceplans table
-- ANSWER 80

SELECT COUNT(1) plan_count
FROM dim_priceplans;

-- QUESTION 2: What segment does the most expensive subscription belong to?

-- Join tables to include all subscription records
-- Ignore NULL subscriptions/rates
-- Order desceding and take the most expensive one and display alonf with segment

-- ANSWER: Business segment, rate 1061.25

SELECT d.product_segment, f.rate
FROM fct_subscriptions f
LEFT JOIN dim_priceplans d
ON f.soc_pp_code = d.soc_pp_code
-- ignore NULL subscription rate
WHERE f.rate is not NULL
ORDER BY f.rate DESC
LIMIT 1;

-- QUESTION 3: How much does the most popular subscription cost?

-- count subscribers of most popular rate

-- ANSWER: Most popular subscription with rate of 299.00, with 229 subscribers

SELECT count(rate) count_of_subscribers, rate most_pop_sub_rate
FROM fct_subscriptions
GROUP BY rate
ORDER BY count(rate) DESC
LIMIT 1;

-- QUESTION 4: How many times did customers switch from a less expensive to a more expensive subscription?

-- create window function to have a field of subscriber partitions ordered by date - have a lagging previuos plan chnage date 
-- count lines where current rate is higher than previuos rate

-- ANSWER: Customers switched 109 times

SELECT
COUNT(rate) change_to_high
FROM (
    SELECT
        *,
        LAG(rate) OVER(PARTITION BY subscriber_id ORDER BY effective_date ASC) prev_rate
    FROM fct_subscriptions
    ) f1
WHERE rate > prev_rate

-- QUESTION 5: Which week of which year did the majority of subscriptions expire?

-- extract year and date
-- group by both and order
-- count expiration_dates and order descending to have majority value

-- ANSWER: Most subscriptions (451) expire on week 7 of 2019

SELECT
    TO_CHAR(expiration_date, 'yyyy') year_num,
    TO_CHAR(expiration_date, 'ww') week_num, 
    COUNT(TO_CHAR(expiration_date, 'WW')) expiration_count
FROM fct_subscriptions
GROUP BY year_num, week_num
ORDER BY expiration_count DESC
LIMIT 1;

-- QUESTION 6: How many new customers have been added on 2018-12-12? How many existing customers renewed their subscriptions on 2018-12-12?

-- assuming old customer appears more than once in fct_subscriptions table
-- get partition of subscribers
-- chose ones that are on 2018-12-12  and appeared once in a table - new customer
-- chose ones that are on 2018-12-12  and appeared more than once in a table - renewal customer

-- ANSWER: On 2018-12-12 there were 3 new customers and 0 customers renewed subscriptions

SELECT
    COUNT(1) new_customers
FROM (
    SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY subscriber_id) transactions
    FROM fct_subscriptions
    ) f1
WHERE DATE(effective_date) = '2018-12-12' AND f1.transactions = 1;


SELECT
    COUNT(1) renewal_customers
FROM (
    SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY subscriber_id) transactions
    FROM fct_subscriptions
    ) f1
WHERE DATE(effective_date) = '2018-12-12' AND f1.transactions > 1;

-- QUESTION 7: Every week of every year lists the most expensive subscription, its number, segment, and rate.

-- Join tables to have all subscriptions
-- Ignore NULLs and find maximum rate
-- Select required fields from subquery expression
-- use corelated subquery to go line by line and compare rate to maximum rate on week basis

-- ANSWER:

-- checking if plan  = subscpiption

with data as(
SELECT 
   d.soc_pp_code,
   f.rate,
   lag(rate) over(partition by d.soc_pp_code order by rate) prev_rate
FROM fct_subscriptions f
LEFT JOIN dim_priceplans d
ON f.soc_pp_code = d.soc_pp_code)

-- select * from data where rate != prev_rate;

SELECT
    *
FROM data
WHERE soc_pp_code = 'PVJA';

-- since plan is not equal to subscription and no subcsription ID is available, I assume subscription = rate, ant 'its number' = week number

SELECT
    f1.rate rate_or_subscription,
    f1.year_num,
    f1.week week_num,
    f1.segment
FROM (
    SELECT
        TO_CHAR(DATE(f.effective_date), 'yyyy') year_num,
        TO_CHAR(DATE(f.effective_date), 'ww') week,
        MAX(f.rate) rate,
        d.product_segment segment
    FROM fct_subscriptions f
    LEFT JOIN dim_priceplans d
    ON f.soc_pp_code = d.soc_pp_code
    WHERE rate is not NULL
    GROUP BY year_num, week, segment
    ORDER BY year_num, week
    ) f1
WHERE rate = (
    SELECT
        MAX(rate)
    FROM (
        SELECT
            TO_CHAR(DATE(f.effective_date), 'ww') week,
            f.rate rate
        FROM fct_subscriptions f
        LEFT JOIN dim_priceplans d
        ON f.soc_pp_code = d.soc_pp_code) f2
    WHERE f1.week=f2.week);


-- OPTION: 2 use TEMP TABLE FOR BETTER READABILITY:
-- create temporary table as join and select needed columns, CTE can not be referenced 2nd time in suquery

DROP TABLE data_table;
CREATE TEMP TABLE data_table AS
SELECT 
    max(rate) rate,
    TO_CHAR(date(f.effective_date), 'yyyy') year_num,
    TO_CHAR(date(f.effective_date), 'ww') week,
    d.product_segment segment
FROM fct_subscriptions f
LEFT JOIN dim_priceplans d
ON f.soc_pp_code = d.soc_pp_code
GROUP BY year_num, week, segment
ORDER BY year_num, week;

-- compare max rate/subscription by year and month
SELECT
    * 
FROM data_table a
WHERE rate = (
    SELECT
        MAX(rate)
    FROM data_table
    WHERE week = a.week)
