-- ============================================================
-- GOLD LAYER — Delivery Fact (fct_delivery)
-- Purpose: Operational logistics with optimized integer Surrogate Keys
-- ============================================================

WITH silver_delivery AS (
    SELECT * FROM {{ ref('delivery') }}
),

final_delivery AS (
    SELECT
        -- Surrogate Keys (Optimized Integer)
        ABS(HASH(delivery_id))                              AS delivery_sk,
        ABS(HASH(order_id))                                 AS order_sk,
        ABS(HASH(driver_id))                                AS driver_sk,
        ABS(HASH(restaurant_id))                            AS restaurant_sk,
        ABS(HASH(customer_id))                              AS customer_sk,

        -- Timestamps
        pickup_time,
        dropoff_time,
        delivery_month,
        delivery_day_of_week,
        pickup_hour,

        -- Location
        pickup_lat,
        pickup_lng,
        dropoff_lat,
        dropoff_lng,

        -- Performance (Durations & Delay)
        actual_time_min                                      AS duration_minutes,
        estimated_time_min                                   AS estimated_minutes,
        actual_time_min                                      AS actual_minutes,
        delay_min                                           AS delay_minutes,
        is_late,

        -- Speed & Distance
        distance_km,
        avg_speed_kmh,

        -- Environment
        weather_condition,
        traffic_level,
        condition_severity,

        -- Interaction
        status                                              AS delivery_status,
        driver_rating,
        driver_rating_bucket,
        tip_usd                                             AS tip_amount,
        tip_bucket,
        late_reason,
        COALESCE(notes, 'None')                             AS driver_notes,

        -- Execution Metrics
        delivery_attempt                                    AS attempt_number,
        is_first_attempt,
        is_redelivery,
        total_attempts_for_order,
        proof_of_delivery,

        -- High-Level Flags
        (status = 'Delivered')                              AS is_completed,
        (status = 'Delivered' AND is_late = FALSE)          AS is_perfect_trip,
        (delay_min > 15)                                    AS contains_severe_delay

    FROM silver_delivery
)

SELECT * FROM final_delivery
