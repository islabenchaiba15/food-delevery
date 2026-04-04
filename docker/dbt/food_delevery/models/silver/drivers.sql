-- ============================================================
-- SILVER LAYER — Drivers Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.DRIVERS
-- Purpose: Clean, normalize, and evaluate driver performance
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'DRIVERS') }}
),

transformed AS (
    SELECT
        -- IDs & Identity
        TRIM(driver_id)                                     AS driver_id,
        INITCAP(TRIM(first_name))                             AS first_name,
        INITCAP(TRIM(last_name))                              AS last_name,
        CONCAT(first_name, ' ', last_name)                    AS full_name,
        LOWER(TRIM(email))                                    AS email,
        TRIM(phone)                                           AS phone,

        -- Location & Zone
        INITCAP(TRIM(city))                                   AS city,
        UPPER(TRIM(state))                                    AS state,
        INITCAP(TRIM(zone))                                   AS zone,

        -- Vehicle
        INITCAP(TRIM(vehicle_type))                           AS vehicle_type,
        UPPER(TRIM(vehicle_plate))                            AS vehicle_plate,
        UPPER(TRIM(license_number))                           AS license_number,

        -- Vehicle capability bucket (useful for large order routing)
        CASE
            WHEN vehicle_type IN ('Van', 'Car')        THEN 'High Capacity'
            WHEN vehicle_type IN ('Motorcycle', 'Scooter') THEN 'Medium Capacity'
            WHEN vehicle_type = 'Bicycle'              THEN 'Low Capacity'
            ELSE 'Unknown'
        END                                                   AS vehicle_capacity,

        -- Hire & Tenure
        TRY_CAST(CAST(hire_date AS VARCHAR) AS DATE)          AS hire_date,
        DATEDIFF('month', TRY_CAST(CAST(hire_date AS VARCHAR) AS DATE), CURRENT_DATE()) 
                                                              AS months_on_platform,

        CASE
            WHEN DATEDIFF('month', TRY_CAST(CAST(hire_date AS VARCHAR) AS DATE), CURRENT_DATE()) >= 36
                THEN 'Veteran'
            WHEN DATEDIFF('month', TRY_CAST(CAST(hire_date AS VARCHAR) AS DATE), CURRENT_DATE()) >= 12
                THEN 'Experienced'
            ELSE 'New'
        END                                                   AS tenure_bucket,

        -- Status & Activity
        TRY_CAST(is_active AS BOOLEAN)                        AS is_active,
        INITCAP(TRIM(status))                                 AS status_raw,

        -- is_active=False but status=Available/On Delivery = inconsistency
        CASE
            WHEN is_active = FALSE AND status_raw IN ('Available', 'On Delivery', 'On Break') THEN 'Inactive'
            ELSE status_raw
        END                                                   AS status_clean,

        -- Background Check
        INITCAP(TRIM(background_check))                       AS background_check,

        -- Performance Metrics
        COALESCE(total_deliveries, 0)                         AS total_deliveries,
        CAST(avg_rating AS NUMERIC(3, 1))                     AS avg_rating,
        CAST(on_time_rate_pct AS NUMERIC(5, 2))                AS on_time_rate_pct,
        CAST(acceptance_rate_pct AS NUMERIC(5, 2))             AS acceptance_rate_pct,

        -- Rating bucket
        CASE
            WHEN avg_rating >= 4.5 THEN 'Excellent'
            WHEN avg_rating >= 4.0 THEN 'Good'
            WHEN avg_rating >= 3.5 THEN 'Average'
            ELSE 'Poor'
        END                                                   AS rating_bucket,

        -- Performance tier combining rating + on-time rate
        CASE
            WHEN avg_rating >= 4.0 AND on_time_rate_pct >= 80 THEN 'Top Performer'
            WHEN avg_rating >= 3.5 AND on_time_rate_pct >= 65 THEN 'Good Performer'
            WHEN avg_rating < 3.0  OR  on_time_rate_pct < 50  THEN 'At Risk'
            ELSE 'Average Performer'
        END                                                   AS performance_tier,

        -- Earnings
        CAST(hourly_rate_usd AS NUMERIC(18, 2))               AS hourly_rate_usd,
        CAST(total_earnings_usd AS NUMERIC(18, 2))            AS total_earnings_usd,
        CAST(total_tips_usd AS NUMERIC(18, 2))                AS total_tips_usd,
        
        -- Avg earning per delivery
        ROUND(total_earnings_usd / NULLIF(total_deliveries, 0), 2) AS avg_earning_per_delivery_usd,

        -- Distance
        max_distance_km,

        -- Data Quality Flags
        (background_check = 'Failed' AND is_active = TRUE)    AS dq_active_failed_background,
        COUNT(driver_id) OVER (PARTITION BY license_number) > 1 AS dq_duplicate_license,
        COUNT(driver_id) OVER (PARTITION BY vehicle_plate) > 1  AS dq_duplicate_plate,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE driver_id IS NOT NULL
)

SELECT * 
FROM transformed
-- Deduplicate: keep only the most recent extract for each driver_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY _extracted_at DESC) = 1
