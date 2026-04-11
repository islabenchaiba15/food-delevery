-- ============================================================
-- SILVER LAYER — Delivery Staging (Clean & Opinionated)
-- Source: FOOD_DELIVERY_DW.BRONZE.DELIVERY
-- Purpose: Clean, normalize, and repair delivery records
--          using best-practice imputation.
--
-- Fix strategy summary:
--   1.  Duration source conflict      → use timestamp diff as truth,
--                                       actual_time_min as fallback
--   2.  is_on_time mismatch           → re-derive from clean duration
--   3.  is_late mismatch              → re-derive from clean duration
--   4.  delay_min                     → recompute from clean duration
--   5.  avg_speed_kmh outliers        → NULL above 120 km/h
--   6.  late_reason inconsistencies   → NULL if on-time,
--                                       'Unknown' if late with no reason
--   7.  Driver rating on Failed/Returned → NULL it
--   8.  Tip on Failed/Returned        → zero it
--   9.  Proof of delivery mismatch    → NULL on non-delivered,
--                                       'Not Captured' on delivered
--   10. condition_severity case bug   → reference INITCAP'd columns
--   11. driver_rating_bucket NULL     → explicit WHEN IS NULL branch
--   12. Duplicate delivery IDs        → keep latest _extracted_at
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'DELIVERY') }}
),

-- ----------------------------------------------------------------
-- STEP 1 — Base casting & string normalization
--           INITCAP applied here so every downstream CTE
--           works on clean, consistent values.
-- ----------------------------------------------------------------
casted AS (
    SELECT
        TRIM(delivery_id)                                       AS delivery_id,
        TRIM(order_id)                                          AS order_id,
        TRIM(driver_id)                                         AS driver_id,
        TRIM(restaurant_id)                                     AS restaurant_id,
        TRIM(customer_id)                                       AS customer_id,

        TRY_CAST(pickup_time  AS TIMESTAMP)                     AS pickup_time,
        TRY_CAST(dropoff_time AS TIMESTAMP)                     AS dropoff_time,

        CAST(estimated_time_min  AS INTEGER)                    AS estimated_time_min,
        CAST(actual_time_min     AS INTEGER)                    AS actual_time_min_raw,
        CAST(distance_km         AS FLOAT)                      AS distance_km,
        CAST(pickup_lat          AS FLOAT)                      AS pickup_lat,
        CAST(pickup_lng          AS FLOAT)                      AS pickup_lng,
        CAST(dropoff_lat         AS FLOAT)                      AS dropoff_lat,
        CAST(dropoff_lng         AS FLOAT)                      AS dropoff_lng,

        INITCAP(TRIM(status))                                   AS status,
        INITCAP(TRIM(weather_condition))                        AS weather_condition,
        INITCAP(TRIM(traffic_level))                            AS traffic_level,

        CAST(delivery_attempt    AS INTEGER)                    AS delivery_attempt,
        TRIM(proof_of_delivery)                                 AS proof_of_delivery_raw,
        CAST(driver_rating       AS NUMERIC(3, 1))              AS driver_rating_raw,
        CAST(COALESCE(tip_usd, 0) AS NUMERIC(18, 2))            AS tip_usd_raw,
        CAST(is_on_time          AS BOOLEAN)                    AS is_on_time_raw,
        TRIM(late_reason)                                       AS late_reason_raw,
        TRIM(notes)                                             AS notes,
        CAST(event_time          AS TIMESTAMP)                  AS _extracted_at
    FROM source
    WHERE delivery_id IS NOT NULL
),

-- ----------------------------------------------------------------
-- STEP 2 — Duration repair (Fix 1)
--           The timestamp diff is the ground truth — it is derived
--           from two objective recorded moments. actual_time_min
--           is a pre-aggregated field that disagrees with the
--           timestamps on 94% of rows.
--           Use timestamp diff where both timestamps exist and
--           produce a positive result; fall back to actual_time_min.
-- ----------------------------------------------------------------
duration_fixed AS (
    SELECT
        *,
        CASE
            WHEN pickup_time IS NOT NULL
                 AND dropoff_time IS NOT NULL
                 AND dropoff_time > pickup_time
            THEN DATEDIFF('minute', pickup_time, dropoff_time)
            ELSE actual_time_min_raw
        END                                                     AS actual_time_min
    FROM casted
),

-- ----------------------------------------------------------------
-- STEP 3 — Derived time flags (Fix 2, 3, 4)
--           Re-derive is_on_time, is_late, and delay_min from the
--           clean actual_time_min computed in the previous step.
--           The source is_on_time column is discarded.
-- ----------------------------------------------------------------
time_flags_fixed AS (
    SELECT
        *,
        (actual_time_min <= estimated_time_min)                 AS is_on_time,
        (actual_time_min >  estimated_time_min)                 AS is_late,
        (actual_time_min -  estimated_time_min)                 AS delay_min
    FROM duration_fixed
),

-- ----------------------------------------------------------------
-- STEP 4 — Speed repair (Fix 5)
--           Food delivery vehicles do not exceed 120 km/h.
--           NULL the speed when implausible rather than capping —
--           the underlying distance or time data is untrustworthy.
-- ----------------------------------------------------------------
speed_fixed AS (
    SELECT
        *,
        CASE
            WHEN NULLIF(actual_time_min, 0) IS NULL THEN NULL
            WHEN ROUND(distance_km / NULLIF(actual_time_min / 60.0, 0), 2) > 120 THEN NULL
            ELSE ROUND(distance_km / NULLIF(actual_time_min / 60.0, 0), 2)
        END                                                     AS avg_speed_kmh
    FROM time_flags_fixed
),

-- ----------------------------------------------------------------
-- STEP 5 — Late reason repair (Fix 6)
--           On-time deliveries should not have a late reason.
--           Late deliveries with no reason recorded → 'Unknown'.
-- ----------------------------------------------------------------
late_reason_fixed AS (
    SELECT
        *,
        CASE
            WHEN is_on_time = TRUE  THEN NULL
            WHEN is_on_time = FALSE
                 AND late_reason_raw IS NULL THEN 'Unknown'
            ELSE late_reason_raw
        END                                                     AS late_reason
    FROM speed_fixed
),

-- ----------------------------------------------------------------
-- STEP 6 — Status-sensitive field repairs (Fix 7, 8, 9)
--
--   Driver rating  → NULL on Failed / Returned (never completed)
--   Tip            → 0 on Failed / Returned (never collected)
--   Proof          → NULL on non-Delivered (noise)
--                    'Not Captured' on Delivered with no proof
-- ----------------------------------------------------------------
status_fixed AS (
    SELECT
        *,

        -- Fix 7: rating only valid when delivery completed
        CASE
            WHEN status IN ('Failed', 'Returned') THEN NULL
            ELSE driver_rating_raw
        END                                                     AS driver_rating,

        -- Fix 8: tip only valid when delivery completed
        CASE
            WHEN status IN ('Failed', 'Returned') THEN 0
            ELSE tip_usd_raw
        END                                                     AS tip_usd,

        -- Fix 9: proof of delivery aligned to status
        CASE
            WHEN status = 'Delivered' AND proof_of_delivery_raw IS NULL
                THEN 'Not Captured'
            WHEN status != 'Delivered'
                THEN NULL
            ELSE proof_of_delivery_raw
        END                                                     AS proof_of_delivery

    FROM late_reason_fixed
),

-- ----------------------------------------------------------------
-- STEP 7 — Condition severity (Fix 10)
--           References the INITCAP'd columns from casted so the
--           comparisons are always case-consistent.
-- ----------------------------------------------------------------
condition_fixed AS (
    SELECT
        *,
        CASE
            WHEN weather_condition IN ('Stormy', 'Snowy')
                 OR traffic_level = 'Severe'   THEN 'Severe'
            WHEN weather_condition IN ('Rainy', 'Windy')
                 OR traffic_level = 'High'      THEN 'High'
            WHEN weather_condition = 'Cloudy'
                 OR traffic_level = 'Moderate'  THEN 'Moderate'
            ELSE 'Normal'
        END                                                     AS condition_severity
    FROM status_fixed
),

-- ----------------------------------------------------------------
-- STEP 8 — Rating bucket (Fix 11)
--           Explicit NULL branch prevents silent misclassification
--           if the ELSE clause is ever modified in future.
-- ----------------------------------------------------------------
rating_bucketed AS (
    SELECT
        *,
        CASE
            WHEN driver_rating IS NULL  THEN 'No Rating'
            WHEN driver_rating >= 4     THEN 'Good'
            WHEN driver_rating = 3      THEN 'Neutral'
            WHEN driver_rating <= 2     THEN 'Bad'
            ELSE 'No Rating'
        END                                                     AS driver_rating_bucket,

        CASE
            WHEN tip_usd = 0    THEN 'No Tip'
            WHEN tip_usd <= 5   THEN 'Low'
            WHEN tip_usd <= 10  THEN 'Medium'
            ELSE 'High'
        END                                                     AS tip_bucket
    FROM condition_fixed
),

-- ----------------------------------------------------------------
-- STEP 9 — Redelivery window aggregations
--           Kept in a separate CTE so window functions run after
--           all row-level repairs are complete.
-- ----------------------------------------------------------------
windowed AS (
    SELECT
        *,
        COUNT(delivery_id) OVER (
            PARTITION BY order_id
        ) > 1                                                   AS is_redelivery,

        MAX(delivery_attempt) OVER (
            PARTITION BY order_id
        )                                                       AS total_attempts_for_order,

        (delivery_attempt = 1)                                  AS is_first_attempt
    FROM rating_bucketed
),

-- ----------------------------------------------------------------
-- STEP 10 — Deduplication (Fix 12)
--            Same delivery_id re-extracted → keep the freshest row.
-- ----------------------------------------------------------------
deduped AS (
    SELECT *
    FROM windowed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY delivery_id
        ORDER BY _extracted_at DESC
    ) = 1
)

-- ----------------------------------------------------------------
-- FINAL — Clean projection only. No raw or intermediate columns.
-- ----------------------------------------------------------------
SELECT
    -- Keys
    delivery_id,
    order_id,
    driver_id,
    restaurant_id,
    customer_id,

    -- Timestamps
    pickup_time,
    dropoff_time,
    DATE_TRUNC('month', pickup_time)                            AS delivery_month,
    DAYNAME(pickup_time)                                        AS delivery_day_of_week,
    HOUR(pickup_time)                                           AS pickup_hour,

    -- Delivery timing (all from clean timestamp-derived duration)
    estimated_time_min,
    actual_time_min,
    delay_min,
    is_on_time,
    is_late,
    late_reason,

    -- Distance & speed
    distance_km,
    avg_speed_kmh,

    -- Status flags
    status,
    (status = 'Delivered')                                      AS is_delivered,
    (status IN ('Failed', 'Returned'))                          AS is_failed,
    (status IN ('In Transit', 'Partial'))                       AS is_in_progress,

    -- Location
    pickup_lat,
    pickup_lng,
    dropoff_lat,
    dropoff_lng,

    -- Conditions
    weather_condition,
    traffic_level,
    condition_severity,

    -- Attempt tracking
    delivery_attempt,
    is_first_attempt,
    is_redelivery,
    total_attempts_for_order,

    -- Proof & notes
    proof_of_delivery,
    notes,

    -- Driver performance
    driver_rating,
    driver_rating_bucket,

    -- Tip
    tip_usd,
    tip_bucket,

    -- Audit
    _extracted_at,
    CURRENT_TIMESTAMP()                                         AS _loaded_at

FROM deduped