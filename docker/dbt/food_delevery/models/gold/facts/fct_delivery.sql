-- ============================================================
-- GOLD LAYER — Delivery Fact (fct_delivery)
-- Purpose: Operational logistics & performance metrics
-- ============================================================

WITH silver_delivery AS (
    SELECT * FROM {{ ref('delivery') }}
),

final_delivery AS (
    SELECT
        -- IDs
        delivery_id,
        order_id,
        driver_id,
        restaurant_id,
        customer_id,

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
        duration_min_calculated                              AS duration_minutes,
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
        tip_amount,
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
