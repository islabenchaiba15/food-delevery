-- ============================================================
-- GOLD LAYER — Promotion Dimension (dim_promotions)
-- Purpose: Marketing campaign monitoring & ROI analysis with Optimized integer Surrogate Keys
-- ============================================================

WITH silver_promotions AS (
    SELECT * FROM {{ ref('promotions') }}
),

final_promotions AS (
    SELECT
        -- Surrogate Key (Optimized Integer)
        ABS(HASH(promo_id))                                 AS promo_sk,
        ABS(HASH(restaurant_id))                            AS restaurant_sk,

        -- IDs & Attribution (Natural IDs preserved)
        campaign_id,
        restaurant_scope,
        created_by                                          AS campaign_manager,

        -- Promo Identity
        promo_code,
        promo_name,
        promo_type,
        target_segment,
        applies_to,
        channel                                             AS marketing_channel,
        is_stackable_flag,

        -- Discount Terms
        discount_value,
        discount_unit_clean                                AS discount_type,
        discount_label,
        min_order_usd,
        max_discount_usd,

        -- Campaign Lifecycle
        start_date,
        end_date,
        duration_days,
        is_currently_live,

        -- Usage Metrics
        usage_limit,
        times_used                                          AS redemption_count,
        usage_rate_pct,
        usage_bucket,

        -- Financial Impact
        revenue_cost_usd                                    AS total_campaign_cost_usd,
        avg_cost_per_use_usd,

        -- Data Quality Flags
        dq_over_usage_limit,
        dq_max_discount_lt_min_order

    FROM silver_promotions
)

SELECT * FROM final_promotions
