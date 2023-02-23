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
-- ANSWER 80
SELECT COUNT(1) plan_count
FROM dim_priceplans;

-- QUESTION 2: What segment does the most expensive subscription belong to?
-- ANSWER: Business segment, rate 1061.25
SELECT d.product_segment, f.rate
FROM fct_subscriptions f
JOIN dim_priceplans d
ON f.soc_pp_code = d.soc_pp_code
WHERE f.rate is not NULL
ORDER BY f.rate DESC
LIMIT 1;

-- QUESTION 3: How much does the most popular subscription cost?
-- ANSWER: Most popular subscription with rate of 299.00, with 229 subscribers
SELECT count(rate) count_of_subscribers, rate most_pop_sub_rate
FROM fct_subscriptions
GROUP BY rate
ORDER BY count(rate) DESC
LIMIT 1;

-- QUESTION 4: How many times did customers switch from a less expensive to a more expensive subscription?
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
-- ANSWER: Most subscriptions (469) expire on week 2 of 2019
SELECT
    TO_CHAR(expiration_date, 'yyyy') year_num,
    TO_CHAR(expiration_date, 'W') week_num, 
    COUNT(TO_CHAR(expiration_date, 'W')) expiration_count
FROM fct_subscriptions
GROUP BY year_num, week_num
ORDER BY expiration_count DESC
LIMIT 1;

-- QUESTION 6: How many new customers have been added on 2018-12-12? How many existing customers renewed their subscriptions on 2018-12-12?
-- ANSWER: On 2018-12-12 there were 3 new customers and 0 customers renewed subscriptions
SELECT
    COUNT(1) new_customers
FROM (
    SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY subscriber_id) rank
    FROM fct_subscriptions
    ) f1
WHERE DATE(effective_date) = '2018-12-12' AND f1.rank = 1;


SELECT
    COUNT(1) renewal_customers
FROM (
    SELECT
        *,
        DENSE_RANK() OVER(PARTITION BY subscriber_id) rank
    FROM fct_subscriptions
    ) f1
WHERE DATE(effective_date) = '2018-12-12' AND f1.rank > 1;

-- QUESTION 7: Every week of every year lists the most expensive subscription, its number, segment, and rate.
-- ANSWER:

SELECT
    f1.rate rate_or_subscription,
    f1.year_num,
    f1.week week_num,
    f1.segment
FROM (
    SELECT
        TO_CHAR(DATE(f.effective_date), 'yyyy') year_num,
        TO_CHAR(DATE(f.effective_date), 'w') week,
        MAX(f.rate) rate, d.product_segment segment
    FROM fct_subscriptions f
    JOIN dim_priceplans d
    ON f.soc_pp_code = d.soc_pp_code
    WHERE rate is not NULL
    GROUP BY year_num, week, segment
    ORDER BY week
    ) f1
WHERE rate = (
    SELECT
        MAX(rate)
    FROM (
        SELECT
            TO_CHAR(DATE(f.effective_date), 'w') week,
            f.rate rate
        FROM fct_subscriptions f
        JOIN dim_priceplans d
        ON f.soc_pp_code = d.soc_pp_code) f2
    WHERE f1.week=f2.week);


