-- ============================================================
-- SILVER LAYER — Customers Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.CUSTOMERS
-- Purpose: Clean, deduplicate, and validate customer profiles
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'CUSTOMERS') }}
),

transformed AS (
    SELECT
        -- IDs
        TRIM(customer_id)                                   AS customer_id,
        TRIM(user_id)                                       AS user_id,

        -- Name & Contact
        INITCAP(TRIM(first_name))                             AS first_name,
        INITCAP(TRIM(last_name))                              AS last_name,
        CONCAT(first_name, ' ', last_name)                    AS full_name,
        LOWER(TRIM(email))                                    AS email,
        REGEXP_REPLACE(phone, '[^0-9]', '')                  AS phone_digits,

        -- Demographic
        TRY_CAST(CAST(date_of_birth AS VARCHAR) AS DATE)     AS date_of_birth,
        DATEDIFF('year', TRY_CAST(CAST(date_of_birth AS VARCHAR) AS DATE), CURRENT_DATE()) AS age_years,
        CASE 
            WHEN LOWER(gender) IN ('male', 'm')   THEN 'Male'
            WHEN LOWER(gender) IN ('female', 'f') THEN 'Female'
            ELSE 'Other'
        END                                                   AS gender,
        
        -- Geography
        INITCAP(TRIM(city))                                   AS city,
        UPPER(TRIM(state))                                    AS state,
        LPAD(CAST(zip_code AS VARCHAR), 5, '0')              AS zip_code,

        -- Membership & Lifecycle
        TRY_CAST(CAST(registration_date AS VARCHAR) AS DATE) AS registered_at,
        TRY_CAST(CAST(last_order_date AS VARCHAR) AS DATE)   AS last_order_at,
        INITCAP(TRIM(account_status))                         AS account_status,

        -- Engagement Metrics
        COALESCE(total_orders, 0)                             AS total_orders,
        TRY_CAST(CAST(COALESCE(total_spent_usd, 0) AS VARCHAR) AS NUMERIC(18, 2)) AS total_spent_usd,
        TRY_CAST(CAST(COALESCE(avg_order_value_usd, 0) AS VARCHAR) AS NUMERIC(18, 2)) AS avg_order_value_usd_raw,
        COALESCE(loyalty_points, 0)                           AS loyalty_points,
        INITCAP(TRIM(loyalty_tier))                           AS loyalty_tier,
        
        -- Preferences
        INITCAP(TRIM(preferred_payment))                      AS preferred_payment_method,
        INITCAP(TRIM(preferred_cuisine))                      AS preferred_cuisine_type,
        COALESCE(NULLIF(TRIM(dietary_preference), 'NaN'), 'None Specified') AS dietary_preference,

        -- Boolean Flags
        TRY_CAST(CAST(is_premium AS VARCHAR) AS BOOLEAN)      AS is_premium,
        TRY_CAST(CAST(marketing_opt_in AS VARCHAR) AS BOOLEAN) AS has_marketing_opt_in,
        
        -- Success/Risk metrics
        TRY_CAST(NULLIF(TRIM(churn_risk), 'NaN') AS FLOAT)    AS churn_risk_score,
        COALESCE(referral_count, 0)                           AS referral_count,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE customer_id IS NOT NULL
),

final_cleaning AS (
    SELECT
        *,
        -- Corrected Average Order Value (Derived from Transformed)
        CASE
            WHEN total_orders > 0 THEN ROUND(total_spent_usd / total_orders, 2)
            ELSE 0.00
        END                                                   AS avg_order_value_corrected,
        
        -- Data Quality Flags
        (total_orders > 0 AND ABS(avg_order_value_usd_raw - ROUND(total_spent_usd / total_orders, 2)) > 0.01)
                                                              AS dq_avg_val_mismatch,
        (total_orders > 0 AND total_spent_usd < 1.00 AND registered_at < CURRENT_DATE - INTERVAL '30 days')
                                                              AS dq_low_spend_anomaly,
        (loyalty_points = 0 AND total_spent_usd > 100)        AS dq_loyalty_mismatch

    FROM transformed
)

SELECT
    customer_id,
    user_id,
    first_name,
    last_name,
    full_name,
    email,
    phone_digits,
    date_of_birth,
    age_years,
    gender,
    city,
    state,
    zip_code,
    registered_at,
    last_order_at,
    account_status,
    total_orders,
    total_spent_usd,
    -- Use corrected average if raw value was flagged
    CASE 
        WHEN dq_avg_val_mismatch THEN avg_order_value_corrected 
        ELSE avg_order_value_usd_raw 
    END                                                       AS avg_order_value_usd,
    loyalty_points,
    loyalty_tier,
    preferred_payment_method,
    preferred_cuisine_type,
    dietary_preference,
    is_premium,
    churn_risk_score                                          AS churn_risk,
    has_marketing_opt_in,
    referral_count,
    -- DQ Monitoring Flags
    dq_avg_val_mismatch,
    dq_low_spend_anomaly,
    dq_loyalty_mismatch,
    -- Audit
    _extracted_at,
    _loaded_at
FROM final_cleaning
-- Deduplicate: keep only the most recent extract for each customer
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _extracted_at DESC) = 1
