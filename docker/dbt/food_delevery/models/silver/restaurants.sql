-- ============================================================
-- SILVER LAYER — Restaurants Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.RESTAURANTS
-- Purpose: Clean, normalize, and evaluate restaurant performance
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'RESTAURANTS') }}
),

transformed AS (
    SELECT
        -- IDs & Identity
        TRIM(restaurant_id)                                 AS restaurant_id,
        INITCAP(TRIM(name))                                   AS restaurant_name,
        INITCAP(TRIM(cuisine_type))                           AS cuisine_type,

        -- Location
        TRIM(address)                                         AS address,
        INITCAP(TRIM(city))                                   AS city,
        UPPER(TRIM(state))                                    AS state,
        LPAD(CAST(zip_code AS VARCHAR), 5, '0')              AS zip_code,

        -- Contact
        TRIM(phone)                                           AS phone,
        LOWER(TRIM(email))                                    AS email,
        LOWER(TRIM(website))                                  AS website,

        -- Plan & Financials
        INITCAP(TRIM(plan_tier))                              AS plan_tier,
        CAST(monthly_fee_usd AS NUMERIC(18, 2))               AS monthly_fee_usd,
        CAST(commission_pct AS NUMERIC(5, 2))                 AS commission_pct,

        -- Commission bucket for Power BI
        CASE
            WHEN commission_pct >= 25 THEN 'High (25%+)'
            WHEN commission_pct >= 18 THEN 'Medium (18-25%)'
            ELSE 'Low (<18%)'
        END                                                   AS commission_bucket,

        -- Signup & Activity
        TRY_CAST(CAST(signup_date AS VARCHAR) AS DATE)       AS signup_date,
        DATEDIFF('month', TRY_CAST(CAST(signup_date AS VARCHAR) AS DATE), CURRENT_DATE()) 
                                                              AS months_on_platform,
        
        CAST(is_active AS BOOLEAN)                            AS is_active,
        CAST(is_featured AS BOOLEAN)                          AS is_featured,

        CASE
            WHEN is_active = FALSE THEN 'Inactive'
            WHEN is_featured = TRUE THEN 'Featured'
            ELSE 'Active'
        END                                                   AS restaurant_status,

        -- Performance Metrics
        CAST(avg_rating AS NUMERIC(3, 1))                     AS avg_rating,
        COALESCE(total_reviews, 0)                             AS total_reviews,
        COALESCE(total_orders, 0)                              AS total_orders,
        CAST(avg_prep_time_min AS INTEGER)                    AS avg_prep_time_min,

        -- Rating bucket
        CASE
            WHEN avg_rating >= 4.5 THEN 'Excellent'
            WHEN avg_rating >= 4.0 THEN 'Good'
            WHEN avg_rating >= 3.5 THEN 'Average'
            ELSE 'Poor'
        END                                                   AS rating_bucket,

        -- Popularity bucket
        CASE
            WHEN total_orders >= 15000 THEN 'High Volume'
            WHEN total_orders >= 8000  THEN 'Medium Volume'
            ELSE 'Low Volume'
        END                                                   AS volume_bucket,

        -- Revenue estimate (total_orders × avg commission)
        ROUND(total_orders * (commission_pct / 100.0), 2)     AS estimated_commissions_earned,

        -- Operating Hours
        TRY_CAST(CAST(open_time AS VARCHAR) AS TIME)          AS open_time,
        TRY_CAST(CAST(close_time AS VARCHAR) AS TIME)         AS close_time,

        -- Daily operating hours
        CASE
            WHEN close_time IN ('00:00', '01:00') THEN (24 + HOUR(TRY_CAST(CAST(close_time AS VARCHAR) AS TIME))) - HOUR(TRY_CAST(CAST(open_time AS VARCHAR) AS TIME))
            ELSE HOUR(TRY_CAST(CAST(close_time AS VARCHAR) AS TIME)) - HOUR(TRY_CAST(CAST(open_time AS VARCHAR) AS TIME))
        END                                                   AS operating_hours_per_day,

        -- Delivery
        delivery_radius_km,
        CAST(min_order_usd AS NUMERIC(10, 2))                 AS min_order_usd,

        -- Feature Flags
        CAST(accepts_cash AS BOOLEAN)                         AS accepts_cash,
        CAST(halal_certified AS BOOLEAN)                      AS halal_certified,
        CAST(vegan_options AS BOOLEAN)                        AS vegan_options,

        -- Data Quality Flags
        (is_active = FALSE AND is_featured = TRUE)            AS dq_inactive_but_featured,
        (total_orders > 10000 AND total_reviews < 10)         AS dq_low_reviews_high_orders,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE restaurant_id IS NOT NULL
)

SELECT * 
FROM transformed
-- Deduplicate: keep only the most recent extract for each restaurant_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY restaurant_id ORDER BY _extracted_at DESC) = 1