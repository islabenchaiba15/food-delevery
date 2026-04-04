-- ============================================================
-- SILVER LAYER — Delivery Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.DELIVERY
-- Purpose: Clean, normalize, and analyze delivery performance
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'DELIVERY') }}
),

transformed AS (
    SELECT
        -- IDs
        TRIM(delivery_id)                                   AS delivery_id,
        TRIM(order_id)                                      AS order_id,
        TRIM(driver_id)                                     AS driver_id,
        TRIM(restaurant_id)                                 AS restaurant_id,
        TRIM(customer_id)                                   AS customer_id,

        -- Timestamps
        TRY_CAST(pickup_time  AS TIMESTAMP)                 AS pickup_time,
        TRY_CAST(dropoff_time AS TIMESTAMP)                 AS dropoff_time,

        -- Actual duration from timestamps (more reliable than actual_time_min)
        DATEDIFF('minute', TRY_CAST(pickup_time AS TIMESTAMP), TRY_CAST(dropoff_time AS TIMESTAMP)) 
                                                            AS duration_min_calculated,

        DATE_TRUNC('month', TRY_CAST(pickup_time AS TIMESTAMP)) AS delivery_month,
        DAYNAME(TRY_CAST(pickup_time AS TIMESTAMP))             AS delivery_day_of_week,
        HOUR(TRY_CAST(pickup_time AS TIMESTAMP))                AS pickup_hour,

        -- Delivery Times (Raw)
        estimated_time_min,
        actual_time_min,

        -- Delay (positive = late, negative = early)
        actual_time_min - estimated_time_min                AS delay_min,

        CASE
            WHEN actual_time_min > estimated_time_min THEN TRUE
            ELSE FALSE
        END                                                 AS is_late,

        -- Distance & Speed
        distance_km,
        ROUND(distance_km / NULLIF(actual_time_min / 60.0, 0), 2)
                                                            AS avg_speed_kmh,

        -- Status
        INITCAP(TRIM(status))                               AS status,
        CASE
            WHEN status = 'Delivered' THEN TRUE
            ELSE FALSE
        END                                                 AS is_delivered,

        CASE
            WHEN status IN ('Failed', 'Returned') THEN TRUE
            ELSE FALSE
        END                                                 AS is_failed,

        CASE
            WHEN status IN ('In Transit', 'Partial') THEN TRUE
            ELSE FALSE
        END                                                 AS is_in_progress,

        -- Location
        pickup_lat,
        pickup_lng,
        dropoff_lat,
        dropoff_lng,

        -- Conditions
        INITCAP(TRIM(weather_condition))                    AS weather_condition,
        INITCAP(TRIM(traffic_level))                        AS traffic_level,

        -- Combined condition severity for analysis
        CASE
            WHEN weather_condition IN ('Stormy', 'Snowy') OR traffic_level = 'Severe' THEN 'Severe'
            WHEN weather_condition IN ('Rainy', 'Windy')  OR traffic_level = 'High'   THEN 'High'
            WHEN weather_condition = 'Cloudy'             OR traffic_level = 'Moderate' THEN 'Moderate'
            ELSE 'Normal'
        END                                                 AS condition_severity,

        -- Delivery Attempt
        delivery_attempt,
        (delivery_attempt = 1)                              AS is_first_attempt,

        -- Proof of Delivery
        COALESCE(TRIM(proof_of_delivery), 'Not Captured')   AS proof_of_delivery,

        -- Driver Rating
        CAST(driver_rating AS NUMERIC(3, 1))                AS driver_rating,
        CASE
            WHEN driver_rating >= 4 THEN 'Good'
            WHEN driver_rating = 3  THEN 'Neutral'
            WHEN driver_rating <= 2 THEN 'Bad'
            ELSE 'No Rating'
        END                                                 AS driver_rating_bucket,

        -- Tip
        CAST(COALESCE(tip_usd, 0) AS NUMERIC(18, 2))        AS tip_amount,
        CASE
            WHEN tip_usd = 0   THEN 'No Tip'
            WHEN tip_usd <= 5  THEN 'Low'
            WHEN tip_usd <= 10 THEN 'Medium'
            ELSE 'High'
        END                                                 AS tip_bucket,

        -- On Time & Late Reason
        CAST(is_on_time AS BOOLEAN)                         AS is_on_time_flag,
        COALESCE(TRIM(late_reason), 'On Time')              AS late_reason,

        -- Notes
        TRIM(notes)                                         AS notes,

        -- Data Quality Flags
        (is_on_time = TRUE  AND actual_time_min > estimated_time_min)   AS dq_on_time_mismatch_late,
        (is_on_time = FALSE AND actual_time_min <= estimated_time_min)  AS dq_on_time_mismatch_early,
        
        -- Redelivery logic (partition by order_id)
        COUNT(delivery_id) OVER (PARTITION BY order_id) > 1         AS is_redelivery,
        MAX(delivery_attempt) OVER (PARTITION BY order_id)          AS total_attempts_for_order,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE delivery_id IS NOT NULL
)

SELECT * 
FROM transformed
-- Deduplicate: keep only the most recent extract for each delivery_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY delivery_id ORDER BY _extracted_at DESC) = 1