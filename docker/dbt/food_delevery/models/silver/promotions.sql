-- ============================================================
-- SILVER LAYER — Promotions Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.PROMOTIONS
-- Purpose: Clean, validate, and analyze promotional campaigns
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'PROMOTIONS') }}
),

transformed AS (
    SELECT
        -- IDs
        TRIM(promo_id)                                       AS promo_id,
        TRIM(campaign_id)                                    AS campaign_id,
        TRIM(created_by)                                     AS created_by,
        TRIM(restaurant_id)                                  AS restaurant_id,

        -- restaurant_id NULL (892 rows) = platform-wide promo, valid
        CASE
            WHEN restaurant_id IS NULL THEN 'Platform-Wide'
            ELSE restaurant_id
        END                                                   AS restaurant_scope,

        -- Promo Identity
        UPPER(TRIM(promo_code))                               AS promo_code,
        INITCAP(TRIM(promo_name))                             AS promo_name,
        INITCAP(TRIM(promo_type))                             AS promo_type,
        INITCAP(TRIM(target_segment))                         AS target_segment,
        INITCAP(TRIM(applies_to))                             AS applies_to,
        INITCAP(TRIM(channel))                                AS channel,
        
        CAST(is_stackable AS BOOLEAN)                         AS is_stackable_flag,

        -- Discount
        CAST(discount_value AS NUMERIC(10, 2))                AS discount_value,
        TRIM(discount_unit)                                   AS discount_unit_raw,

        -- Standardize unit label
        CASE
            WHEN discount_unit_raw = 'Percent'   THEN 'Percentage'
            WHEN discount_unit_raw = 'Fixed USD' THEN 'Fixed'
            ELSE discount_unit_raw
        END                                                   AS discount_unit_clean,

        -- Human-readable discount label
        CASE
            WHEN discount_unit_raw = 'Percent'   THEN CONCAT(discount_value, '%')
            WHEN discount_unit_raw = 'Fixed USD' THEN CONCAT('$', discount_value)
        END                                                   AS discount_label,

        CAST(min_order_usd AS NUMERIC(10, 2))                 AS min_order_usd,
        
        -- Fix: max_discount_usd < min_order_usd (6,820 rows)
        -- If the cap is lower than the minimum order, set cap = min_order_usd
        CASE
            WHEN max_discount_usd < min_order_usd THEN min_order_usd
            ELSE max_discount_usd
        END                                                   AS max_discount_usd,

        -- Dates
        TRY_CAST(CAST(start_date AS VARCHAR) AS DATE)         AS start_date,
        TRY_CAST(CAST(end_date AS VARCHAR) AS DATE)           AS end_date,
        
        DATEDIFF('day', TRY_CAST(CAST(start_date AS VARCHAR) AS DATE), TRY_CAST(CAST(end_date AS VARCHAR) AS DATE)) 
                                                              AS duration_days,

        -- Is the promo currently live based on actual dates?
        CASE
            WHEN CURRENT_DATE() BETWEEN TRY_CAST(CAST(start_date AS VARCHAR) AS DATE) 
                                    AND TRY_CAST(CAST(end_date AS VARCHAR) AS DATE) THEN TRUE
            ELSE FALSE
        END                                                   AS is_currently_live,

        -- Usage
        COALESCE(usage_limit, 0)                              AS usage_limit,
        
        -- FIX: times_used > usage_limit (4,991 rows)
        LEAST(times_used, usage_limit)                        AS times_used,
        times_used                                            AS times_used_original,

        -- Usage rate 
        ROUND(LEAST(times_used, COALESCE(usage_limit,0)) * 100.0 / NULLIF(usage_limit, 0), 2) AS usage_rate_pct,

        CASE
            WHEN LEAST(times_used, usage_limit) >= usage_limit       THEN 'Exhausted'
            WHEN LEAST(times_used, usage_limit) >= usage_limit * 0.8 THEN 'Nearly Exhausted'
            WHEN LEAST(times_used, usage_limit) >= usage_limit * 0.5 THEN 'Mid Usage'
            ELSE 'Low Usage'
        END                                                   AS usage_bucket,

        -- Revenue Impact
        CAST(revenue_impact_usd AS NUMERIC(18, 2))             AS revenue_impact_usd,
        ABS(revenue_impact_usd)                                AS revenue_cost_usd,
        ROUND(ABS(revenue_impact_usd) / NULLIF(LEAST(times_used, usage_limit), 0), 2) AS avg_cost_per_use_usd,

        -- Data Quality Flags
        (times_used > usage_limit)                            AS dq_over_usage_limit,
        (max_discount_usd < min_order_usd)                    AS dq_max_discount_lt_min_order,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE promo_id IS NOT NULL 
)

SELECT * 
FROM transformed
-- Deduplicate: keep only the most recent extract for each promo_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_id ORDER BY _extracted_at DESC) = 1