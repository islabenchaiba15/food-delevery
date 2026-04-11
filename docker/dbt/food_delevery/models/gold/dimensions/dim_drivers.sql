-- ============================================================
-- GOLD LAYER — Driver Dimension (dim_drivers)
-- Purpose: Unified driver profile with optimized integer Surrogate Keys
-- ============================================================

WITH silver_drivers AS (
    SELECT * FROM {{ ref('drivers') }}
),

final_drivers AS (
    SELECT
        -- Surrogate Key (Optimized Integer)
        ABS(HASH(driver_id))                                AS driver_sk,

        -- IDs & Identity (Natural ID preserved)
        full_name,
        email,
        phone,

        -- Location
        city,
        state,
        zone,

        -- Vehicle Details
        vehicle_type,
        vehicle_plate,
        license_number,
        vehicle_capacity,

        -- Lifecycle & Tenure
        hire_date,
        months_on_platform,
        tenure_bucket,
        is_active,
        status_clean                                        AS activity_status,

        -- Risk & Compliance
        background_check,
        dq_active_failed_background                         AS is_high_risk_driver,

        -- Performance Summary
        total_deliveries,
        avg_rating,
        on_time_rate_pct,
        acceptance_rate_pct,
        rating_bucket,
        performance_tier,

        -- Earnings Summary (Cumulative)
        hourly_rate_usd,
        total_earnings_usd,
        total_tips_usd,
        avg_earning_per_delivery_usd,

        -- Delivery Capabilities
        max_distance_km

    FROM silver_drivers
)

SELECT * FROM final_drivers
