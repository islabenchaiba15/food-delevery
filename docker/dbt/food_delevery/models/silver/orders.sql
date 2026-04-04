WITH raw_orders AS (
    SELECT * FROM {{ source('food_delivery_raw', 'ORDERS') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        restaurant_id,
        driver_id,
        CAST(order_date AS DATE) AS order_date,
        CAST(order_time AS TIME) AS order_time,
        -- Combine date and time into a single timestamp
        TIMESTAMP_FROM_PARTS(CAST(order_date AS DATE), CAST(order_time AS TIME)) AS order_at,
        status,
        channel,
        order_type,
        CAST(subtotal_usd AS NUMERIC(18, 2)) AS subtotal_amount,
        CAST(tax_usd AS NUMERIC(18, 2)) AS tax_amount,
        CAST(delivery_fee_usd AS NUMERIC(18, 2)) AS delivery_fee_amount,
        CAST(discount_usd AS NUMERIC(18, 2)) AS discount_amount,
        CAST(total_usd AS NUMERIC(18, 2)) AS total_paid_amount,
        CAST(items_count AS INTEGER) AS items_count,
        promo_code,
        CAST(estimated_delivery_min AS INTEGER) AS estimated_delivery_minutes,
        CAST(actual_delivery_min AS INTEGER) AS actual_delivery_minutes,
        CAST(prep_time_min AS INTEGER) AS prep_time_minutes,
        CAST(customer_rating AS NUMERIC(3, 1)) AS customer_rating,
        notes,
        CAST(is_reorder AS BOOLEAN) AS is_reorder_flag,
        CAST(tip_usd AS NUMERIC(18, 2)) AS tip_amount,
        CAST(refund_amount_usd AS NUMERIC(18, 2)) AS refund_amount,
        COALESCE(cancellation_reason, 'not cancelled') AS cancellation_reason,
        -- Audit columns
        CAST(event_time AS TIMESTAMP) AS _loaded_at
    FROM raw_orders
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned
-- Deduplicate by taking the latest event_time for each order_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1