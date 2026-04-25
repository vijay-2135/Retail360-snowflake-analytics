-- Project  : Retail360 Analytics Platform
-- Author   : Vijay R K
-- Tool     : Snowflake
-- Details  : End-to-end retail data pipeline
--            CSV → Stage → RAW → VALIDATED → CURATED


-- RAW LAYER
use schema raw;

CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id    STRING,
    customer_name  STRING,
    email          STRING,
    phone          STRING,
    gender         STRING,
    age            STRING,
    city           STRING,
    state          STRING,
    loyalty_tier   STRING,
    signup_date    STRING,
    status         STRING
);

CREATE OR REPLACE TABLE RAW.PRODUCTS (
    product_id     STRING,
    product_name   STRING,
    category       STRING,
    sub_category   STRING,
    brand          STRING,
    cost_price     STRING,
    selling_price  STRING,
    status         STRING
);

CREATE OR REPLACE TABLE RAW.INVENTORY (
    store_id           STRING,
    product_id         STRING,
    available_qty      STRING,
    reorder_level      STRING,
    last_restock_date  STRING
);

CREATE OR REPLACE TABLE RAW.SALES (
    txn_id           STRING,
    customer_id      STRING,
    store_id         STRING,
    product_id       STRING,
    txn_timestamp    STRING,
    channel          STRING,
    payment_mode     STRING,
    quantity         STRING,
    unit_price       STRING,
    discount_amount  STRING,
    total_amount     STRING,
    is_return        STRING,
    is_flagged       STRING
);

CREATE OR REPLACE FILE FORMAT RAW.CSV_FORMAT
    TYPE                      = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER               = 1
    EMPTY_FIELD_AS_NULL       = TRUE
    NULL_IF                   = ('NULL', 'null', '', 'unknown', 'N/A');

COPY INTO RETAIL_STORE.RAW.CUSTOMERS
FROM @my_s3_stage/customers_master_dirty_01042026.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO RETAIL_STORE.RAW.PRODUCTS
FROM @my_s3_stage/products_master_dirty_01042026.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO RETAIL_STORE.RAW.INVENTORY
FROM @my_s3_stage/inventory_master_dirty_01042026.csv
FILE_FORMAT = CSV_FORMAT;

COPY INTO RETAIL_STORE.RAW.SALES
FROM @my_s3_stage/sales_master_dirty_01042026.csv
FILE_FORMAT = CSV_FORMAT;

-- ==================
-- VALIDATED LAYER
-- ==================

use schema validated;

CREATE OR REPLACE TABLE VALIDATED.CUSTOMERS AS
SELECT DISTINCT
    UPPER(TRIM(customer_id))                                         AS customer_id,
    INITCAP(TRIM(customer_name))                                     AS customer_name,
    CASE
        WHEN REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
        THEN LOWER(TRIM(email))
        ELSE 'INVALID EMAIL'
    END                                                              AS email,
    CASE
        WHEN REGEXP_LIKE(phone, '^[6-9][0-9]{9}$') THEN phone
        ELSE 'INVALID PHONE'
    END                                                              AS phone,
    COALESCE(NULLIF(TRIM(gender), ''), 'UNKNOWN')                    AS gender,
    COALESCE(TRY_CAST(age AS NUMBER), 0)                             AS age,
    INITCAP(TRIM(city))                                              AS city,
    UPPER(TRIM(state))                                               AS state,
    UPPER(TRIM(loyalty_tier))                                        AS loyalty_tier,
    COALESCE(TRY_TO_DATE(signup_date, 'YYYY-MM-DD'), '1900-01-01')  AS signup_date,
    UPPER(TRIM(status))                                              AS status
FROM RETAIL_STORE.RAW.CUSTOMERS
WHERE customer_id IS NOT NULL;

CREATE OR REPLACE TABLE VALIDATED.PRODUCTS AS
SELECT DISTINCT
    UPPER(TRIM(product_id))                                       AS product_id,
    INITCAP(TRIM(product_name))                                   AS product_name,
    INITCAP(TRIM(category))                                       AS category,
    INITCAP(TRIM(sub_category))                                   AS sub_category,
    COALESCE(NULLIF(TRIM(brand), ''), 'UNKNOWN')                  AS brand,
    ROUND(TRY_CAST(cost_price AS NUMBER(12,2)), 2)                AS cost_price,
    CASE
        WHEN TRY_CAST(selling_price AS NUMBER(12,2)) IS NULL THEN 0
        WHEN TRY_CAST(selling_price AS NUMBER(12,2)) < 0     THEN 0
        ELSE ROUND(TRY_CAST(selling_price AS NUMBER(12,2)), 2)
    END                                                           AS selling_price,
    COALESCE(NULLIF(UPPER(TRIM(status)), ''), 'UNKNOWN')          AS status
FROM RETAIL_STORE.RAW.PRODUCTS
WHERE product_id IS NOT NULL;

CREATE OR REPLACE TABLE VALIDATED.INVENTORY AS
SELECT DISTINCT
    UPPER(TRIM(store_id))                                         AS store_id,
    COALESCE(NULLIF(UPPER(TRIM(product_id)), ''), 'UNKNOWN')      AS product_id,
    CASE
        WHEN TRY_CAST(available_qty AS NUMBER) IS NULL            THEN 0
        WHEN TRY_CAST(available_qty AS NUMBER) < 0                THEN 0
        ELSE TRY_CAST(available_qty AS NUMBER)
    END                                                           AS available_qty,
    CASE
        WHEN TRY_CAST(reorder_level AS NUMBER) IS NULL            THEN 0
        WHEN TRY_CAST(reorder_level AS NUMBER) < 0                THEN 0
        ELSE TRY_CAST(reorder_level AS NUMBER)
    END                                                           AS reorder_level,
    TRY_TO_DATE(last_restock_date, 'YYYY-MM-DD')                  AS last_restock_date
FROM RETAIL_STORE.RAW.INVENTORY
WHERE store_id IS NOT NULL;

CREATE OR REPLACE TABLE VALIDATED.SALES AS
SELECT DISTINCT
    UPPER(TRIM(txn_id))                                           AS txn_id,
    COALESCE(NULLIF(UPPER(TRIM(customer_id)), ''), 'UNKNOWN')     AS customer_id,
    UPPER(TRIM(store_id))                                         AS store_id,
    UPPER(TRIM(product_id))                                       AS product_id,
    COALESCE(TRY_TO_TIMESTAMP_NTZ(txn_timestamp),
             '1900-01-01 00:00:00')                               AS txn_timestamp,
    COALESCE(TRY_TO_DATE(txn_timestamp),
             '1900-01-01')                                        AS txn_date,
    INITCAP(TRIM(channel))                                        AS channel,
    COALESCE(NULLIF(INITCAP(TRIM(payment_mode)), ''), 'UNKNOWN')  AS payment_mode,
    TRY_CAST(quantity AS NUMBER)                                  AS quantity,
    CASE
        WHEN TRY_CAST(unit_price AS NUMBER) IS NULL               THEN 0
        WHEN TRY_CAST(unit_price AS NUMBER) < 0                   THEN 0
        ELSE TRY_CAST(unit_price AS NUMBER)
    END                                                           AS unit_price,
    COALESCE(TRY_CAST(discount_amount AS NUMBER(12,2)), 0)        AS discount_amount,
    COALESCE(TRY_CAST(total_amount AS NUMBER(12,2)), 0)           AS total_amount,
    CASE
        WHEN UPPER(TRIM(is_return)) IN ('Y', '1', 'YES') THEN 'Y'
        WHEN UPPER(TRIM(is_return)) IN ('N', '0', 'NO')  THEN 'N'
        ELSE 'UNKNOWN'
    END                                                           AS is_return,
    UPPER(TRIM(is_flagged))                                       AS is_flagged
FROM RETAIL_STORE.RAW.SALES
WHERE txn_id IS NOT NULL;

-- ==================
-- CURATED LAYER
-- ==================

use schema curated;

CREATE OR REPLACE TABLE CURATED.DIM_CUSTOMERS AS
SELECT
    customer_id,
    customer_name,
    email,
    phone,
    gender,
    age,
    city,
    state,
    loyalty_tier,
    signup_date,
    status
FROM RETAIL_STORE.VALIDATED.CUSTOMERS;

CREATE OR REPLACE TABLE CURATED.DIM_PRODUCTS AS
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    cost_price,
    selling_price,
    ROUND((selling_price - cost_price) / NULLIF(cost_price, 0) * 100, 2) AS margin_pct,
    status
FROM RETAIL_STORE.VALIDATED.PRODUCTS;

CREATE OR REPLACE TABLE CURATED.DIM_STORE AS
SELECT DISTINCT
    store_id,
    REPLACE(store_id, 'STR0', '') AS store_number
FROM RETAIL_STORE.VALIDATED.SALES
ORDER BY store_id;

CREATE OR REPLACE TABLE CURATED.DIM_DATE AS
SELECT DISTINCT
    txn_date                                          AS date_id,
    DATE_PART('DAY',     txn_date)                    AS day,
    DATE_PART('MONTH',   txn_date)                    AS month,
    MONTHNAME(txn_date)                               AS month_name,
    DATE_PART('QUARTER', txn_date)                    AS quarter,
    DATE_PART('YEAR',    txn_date)                    AS year,
    DAYNAME(txn_date)                                 AS day_name,
    CASE
        WHEN DAYNAME(txn_date) IN ('Sat', 'Sun')
        THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END                                               AS day_type
FROM RETAIL_STORE.VALIDATED.SALES
WHERE txn_date != '1900-01-01'
ORDER BY date_id;

CREATE OR REPLACE TABLE CURATED.FACT_SALES AS
SELECT
    s.txn_id,
    s.customer_id,
    s.product_id,
    s.store_id,
    s.txn_date,
    s.channel,
    s.payment_mode,
    s.quantity,
    s.unit_price,
    s.discount_amount,
    s.total_amount,
    ROUND(s.quantity * s.unit_price, 2)                     AS gross_amount,
    ROUND(s.total_amount - (s.quantity * p.cost_price), 2)  AS gross_profit
FROM RETAIL_STORE.VALIDATED.SALES s
LEFT JOIN RETAIL_STORE.VALIDATED.PRODUCTS p
    ON s.product_id = p.product_id;
