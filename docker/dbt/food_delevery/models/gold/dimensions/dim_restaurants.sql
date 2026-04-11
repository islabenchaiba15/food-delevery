-- ============================================================
-- GOLD LAYER — Restaurant Dimension (dim_restaurants)
-- Purpose: Unified merchant profile with optimized integer Surrogate Keys
-- ============================================================

WITH restaurants AS (
    SELECT * FROM {{ ref('restaurants') }}
),

billing_summary AS (
    SELECT 
        restaurant_id,
        SUM(total_usd)                                      AS lifetime_fees_paid_usd,
        SUM(base_fee_usd)                                   AS total_base_fees_usd,
        SUM(overage_fee_usd)                                 AS total_overage_fees_usd,
        MAX(invoice_date)                                   AS last_billing_date,
        COUNT(invoice_id)                                   AS total_invoices_paid
    FROM {{ ref('billing') }}
    WHERE status = 'Paid'
    GROUP BY 1
),

joined AS (
    SELECT
        -- Surrogate Key (Optimized Integer)
        ABS(HASH(r.restaurant_id))                         AS restaurant_sk,
        
        -- IDs & Identity (Natural ID preserved)
        r.restaurant_id,
        r.restaurant_name,
        r.cuisine_type,
        
        -- Location
        r.address,
        r.city,
        r.state,
        r.zip_code,

        -- Contact
        r.phone,
        r.email,
        r.website,

        -- Financial Profile
        r.plan_tier,
        r.monthly_fee_usd,
        r.commission_pct,
        r.commission_bucket,
        COALESCE(b.lifetime_fees_paid_usd, 0)                AS lifetime_fees_paid_usd,
        COALESCE(b.total_base_fees_usd, 0)                   AS total_base_fees_usd,
        COALESCE(b.total_overage_fees_usd, 0)                AS total_overage_fees_usd,
        
        -- Success Metrics
        r.total_orders                                      AS lifetime_orders_count,
        r.total_reviews                                      AS lifetime_reviews_count,
        r.avg_rating,
        r.rating_bucket,
        r.volume_bucket,
        
        -- Operational
        r.avg_prep_time_min,
        r.open_time,
        r.close_time,
        r.operating_hours_per_day,
        r.delivery_radius_km,
        r.min_order_usd,
        
        -- Lifecycle
        r.signup_date,
        r.months_on_platform,
        r.restaurant_status,
        r.is_active,
        r.is_featured,
        b.last_billing_date,

        -- Features
        r.accepts_cash,
        r.halal_certified,
        r.vegan_options

    FROM restaurants r
    LEFT JOIN billing_summary b ON r.restaurant_id = b.restaurant_id
),

final_segmentation AS (
    SELECT
        *,
        -- Merchant Value
        CASE
            WHEN lifetime_fees_paid_usd >= 10000 THEN 'Key Account'
            WHEN lifetime_fees_paid_usd >= 2000  THEN 'Growing Partner'
            ELSE 'Standard Partner'
        END                                                 AS partner_segment,

        -- Operational Efficiency
        CASE
            WHEN avg_prep_time_min <= 15 THEN 'Fast Prep'
            WHEN avg_prep_time_min <= 30 THEN 'Average Prep'
            ELSE 'Slow Prep'
        END                                                 AS efficiency_segment

    FROM joined
)

SELECT * FROM final_segmentation
