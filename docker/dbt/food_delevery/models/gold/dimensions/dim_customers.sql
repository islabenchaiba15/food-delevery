-- ============================================================
-- GOLD LAYER — Customer Dimension (dim_customers)
-- Purpose: Unified customer profile for BI and analytics
-- ============================================================

WITH customers AS (
    SELECT * FROM {{ ref('customers') }}
),

users AS (
    SELECT 
        user_id,
        last_login_at,
        is_email_verified,
        user_role,
        has_notification_opt_in
    FROM {{ ref('users') }}
),

joined AS (
    SELECT
        -- IDs
        c.customer_id,
        c.user_id,

        -- Profile
        c.full_name,
        c.email,
        c.phone_digits                                      AS phone,
        c.date_of_birth,
        c.age_years,
        c.gender,
        
        -- Location
        c.city,
        c.state,
        c.zip_code,

        -- Account Flags
        c.account_status,
        u.is_email_verified,
        u.user_role,
        u.has_notification_opt_in,
        c.is_premium,
        c.has_marketing_opt_in,

        -- Temporal
        c.registered_at,
        c.last_order_at,
        u.last_login_at,
        DATEDIFF('day', c.last_order_at, CURRENT_DATE())    AS days_since_last_order,

        -- Metrics
        c.total_orders,
        c.total_spent_usd                                   AS lifetime_value_usd,
        c.avg_order_value_usd                               AS aov_usd,
        c.loyalty_points,
        c.loyalty_tier,
        c.churn_risk                                        AS churn_risk_score,
        c.referral_count,

        -- Favorites
        c.preferred_payment_method,
        c.preferred_cuisine_type,
        c.dietary_preference

    FROM customers c
    LEFT JOIN users u ON c.user_id = u.user_id
),

final_segmentation AS (
    SELECT
        *,
        -- Life Cycle Segment
        CASE
            WHEN days_since_last_order <= 30 THEN 'Active'
            WHEN days_since_last_order <= 90 THEN 'At Risk'
            WHEN days_since_last_order > 90  THEN 'Churned'
            ELSE 'Unknown'
        END                                                 AS customer_lifecycle,

        -- Value Segment (LTV based)
        CASE
            WHEN lifetime_value_usd >= 500 THEN 'High Value'
            WHEN lifetime_value_usd >= 150 THEN 'Medium Value'
            ELSE 'Low Value'
        END                                                 AS value_segment,

        -- Loyalty Level
        CASE
            WHEN loyalty_tier IN ('Diamond', 'Platinum') THEN 'Top Tier'
            WHEN loyalty_tier IN ('Gold', 'Silver')      THEN 'Mid Tier'
            ELSE 'Base Tier'
        END                                                 AS loyalty_segment

    FROM joined
)

SELECT * FROM final_segmentation
