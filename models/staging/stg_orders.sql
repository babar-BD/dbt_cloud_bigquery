--{{ config(materialized = 'table', alias = 'final_orders_table', schema = 'staging') }}

WITH source AS (

    -- read the table we created in BigQuery
    SELECT
        order_id,
        customer_name,
        product,
        quantity,
        price,
        order_date
    FROM {{ source('bigquery_source', 'orders') }}

),

cleaned AS (

    SELECT
        CAST(order_id AS INT64) AS order_id,
        TRIM(customer_name) AS customer_name,
        TRIM(product) AS product,
        CAST(quantity AS INT64) AS quantity,
        CAST(price AS NUMERIC) AS price,
        order_date,
        
        -- Derived fields
        quantity * price AS total_amount,
        FORMAT_DATE('%Y-%m', order_date) AS order_month
    FROM source

)

SELECT *
FROM cleaned
