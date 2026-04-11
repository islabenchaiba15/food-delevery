-- ============================================================
-- KPI MART — Food Delivery Platform Revenue Summary
-- Purpose: Executive finance dashboard for platform revenue,
--          customer segmentation, and operational health.
-- Grain:   One row per (order_date × customer_segment × restaurant)
-- ============================================================


-- ============================================================
-- STEP 1: Base Orders
-- ============================================================
WITH orders AS (
    SELECT * FROM {{ ref('fct_orders') }}
),


-- ============================================================
-- STEP 2: Payment deduplication
-- For each order, determine if ANY payment succeeded and
-- pick the most recent payment method/gateway reliably.
-- ============================================================
payments_ranked AS (
    SELECT
        order_sk,
        payment_status,
        payment_method,
        payment_gateway,
        refund_amount,
        ROW_NUMBER() OVER (
            PARTITION BY order_sk
            ORDER BY payment_date DESC NULLS LAST
        ) AS rn
    FROM {{ ref('fct_payments') }}
),

payments_summary AS (
    SELECT
        order_sk,
        MAX(CASE WHEN payment_status = 'Success' THEN 1 ELSE 0 END) AS has_success_payment,
        MAX(CASE WHEN rn = 1 THEN payment_method  END)              AS latest_payment_method,
        MAX(CASE WHEN rn = 1 THEN payment_gateway END)              AS latest_payment_gateway,
        SUM(refund_amount)                                          AS total_gateway_refund_usd
    FROM payments_ranked
    GROUP BY order_sk
),


-- ============================================================
-- STEP 3: Customer Details
-- ============================================================
customers AS (
    SELECT
        customer_sk,
        full_name,
        value_segment       AS customer_value_segment,
        customer_lifecycle,
        city                AS customer_city,
        state               AS customer_state
    FROM {{ ref('dim_customers') }}
),


-- ============================================================
-- STEP 4: Restaurant Details
-- ============================================================
restaurants AS (
    SELECT
        restaurant_sk,
        restaurant_name,
        cuisine_type,
        plan_tier           AS merchant_tier,
        city                AS restaurant_city,
        commission_pct
    FROM {{ ref('dim_restaurants') }}
),


-- ============================================================
-- STEP 5: Enriched order rows
-- ============================================================
joined AS (
    SELECT
        o.order_sk,
        o.order_date,

        -- Date helpers
        DATE_TRUNC('week',  o.order_date)               AS order_week,
        DATE_TRUNC('month', o.order_date)               AS order_month,

        -- Dimensions
        c.customer_value_segment,
        c.customer_lifecycle,
        c.customer_city,
        c.customer_state,
        r.restaurant_name,
        r.cuisine_type,
        r.merchant_tier,
        r.restaurant_city,
        o.order_status,
        p.latest_payment_method,
        p.latest_payment_gateway,

        -- Money
        o.subtotal_amount,
        o.tax_amount,
        o.delivery_fee_amount,
        o.discount_amount,
        o.tip_amount,
        o.total_paid_amount                                         AS booking_value_usd,
        o.refund_amount                                             AS order_refund_amount,
        (o.subtotal_amount * COALESCE(r.commission_pct, 0) / 100.0) AS estimated_commission_usd,

        -- Boolean flags
        (o.order_status = 'Delivered')                              AS is_realized_revenue,
        (o.order_status IN ('Cancelled', 'Rejected'))               AS is_cancelled,
        (o.order_status = 'Failed')                                 AS is_failed,
        (p.has_success_payment = 1)                                 AS is_fully_paid

    FROM orders o
    LEFT JOIN payments_summary p   ON o.order_sk     = p.order_sk
    LEFT JOIN customers c          ON o.customer_sk  = c.customer_sk
    LEFT JOIN restaurants r        ON o.restaurant_sk = r.restaurant_sk
),


-- ============================================================
-- STEP 6: Daily aggregation
-- ============================================================
daily_summary AS (
    SELECT
        order_date,
        order_week,
        order_month,

        -- Dimensions
        customer_value_segment,
        customer_lifecycle,
        customer_city,
        customer_state,
        restaurant_name,
        cuisine_type,
        merchant_tier,
        restaurant_city,
        latest_payment_method,

        -- 1. ORDER VOLUMES
        COUNT(order_sk)                                             AS total_bookings_count,
        SUM(CASE WHEN is_realized_revenue = TRUE  THEN 1 ELSE 0 END) AS successful_orders_count,
        SUM(CASE WHEN is_cancelled        = TRUE  THEN 1 ELSE 0 END) AS cancelled_orders_count,
        SUM(CASE WHEN is_failed           = TRUE  THEN 1 ELSE 0 END) AS failed_orders_count,

        -- 2. GROSS BOOKINGS
        SUM(booking_value_usd)                                      AS gross_bookings_usd,

        -- 3. GROSS REVENUE (Delivered only)
        SUM(CASE WHEN is_realized_revenue = TRUE THEN booking_value_usd   ELSE 0 END) AS gross_revenue_usd,

        -- 4. NET REVENUE
        SUM(CASE WHEN is_realized_revenue = TRUE
                 THEN (booking_value_usd - order_refund_amount)
                 ELSE 0 END)                                        AS net_revenue_usd,

        -- 5. REFUNDS & COMMISSION
        SUM(CASE WHEN is_realized_revenue = TRUE THEN order_refund_amount       ELSE 0 END) AS total_refunds_usd,
        SUM(CASE WHEN is_realized_revenue = TRUE THEN estimated_commission_usd  ELSE 0 END) AS platform_commission_usd,
        SUM(CASE WHEN is_realized_revenue = TRUE THEN delivery_fee_amount       ELSE 0 END) AS platform_delivery_fees_usd,

        -- 6. NET PLATFORM INCOME
        SUM(CASE WHEN is_realized_revenue = TRUE
                 THEN (estimated_commission_usd + delivery_fee_amount - order_refund_amount)
                 ELSE 0 END)                                        AS net_income_usd,

        -- 7. PASS-THROUGHS
        SUM(discount_amount)                                        AS total_marketing_discounts_usd,
        SUM(CASE WHEN is_realized_revenue = TRUE THEN tip_amount ELSE 0 END) AS total_tips_pass_through_usd,

        -- 8. KPI RATES
        ROUND(
            SUM(CASE WHEN is_realized_revenue = TRUE THEN booking_value_usd ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_realized_revenue = TRUE THEN 1 ELSE 0 END), 0),
        2)                                                          AS avg_order_value_usd,

        ROUND(
            SUM(CASE WHEN is_realized_revenue = TRUE THEN subtotal_amount ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_realized_revenue = TRUE THEN 1 ELSE 0 END), 0),
        2)                                                          AS avg_subtotal_usd,

        ROUND(
            SUM(CASE WHEN is_realized_revenue = TRUE THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(order_sk), 0),
        2)                                                          AS order_success_rate_pct,

        ROUND(
            SUM(CASE WHEN is_cancelled = TRUE THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(order_sk), 0),
        2)                                                          AS cancellation_rate_pct,

        ROUND(
            SUM(CASE WHEN is_failed = TRUE THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(order_sk), 0),
        2)                                                          AS failure_rate_pct,

        ROUND(
            SUM(CASE WHEN is_realized_revenue = TRUE THEN order_refund_amount ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN is_realized_revenue = TRUE THEN booking_value_usd ELSE 0 END), 0),
        2)                                                          AS refund_rate_pct,

        ROUND(
            SUM(CASE WHEN is_fully_paid = TRUE THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(order_sk), 0),
        2)                                                          AS payment_match_rate_pct,

        ROUND(
            SUM(CASE WHEN is_realized_revenue = TRUE THEN estimated_commission_usd ELSE 0 END) * 100.0
            / NULLIF(SUM(CASE WHEN is_realized_revenue = TRUE THEN subtotal_amount ELSE 0 END), 0),
        2)                                                          AS effective_commission_rate_pct

    FROM joined
    GROUP BY
        order_date,
        order_week,
        order_month,
        customer_value_segment,
        customer_lifecycle,
        customer_city,
        customer_state,
        restaurant_name,
        cuisine_type,
        merchant_tier,
        restaurant_city,
        latest_payment_method
),


-- ============================================================
-- STEP 7: Period-over-period growth (WoW / MoM)
-- ============================================================
period_comparison AS (
    SELECT
        *,

        LAG(gross_revenue_usd, 7) OVER (
            PARTITION BY customer_value_segment, cuisine_type, merchant_tier
            ORDER BY order_date
        )                                                           AS gross_revenue_usd_prior_week,

        ROUND(
            (gross_revenue_usd
             - LAG(gross_revenue_usd, 7) OVER (
                   PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                   ORDER BY order_date
               )
            ) * 100.0
            / NULLIF(
                LAG(gross_revenue_usd, 7) OVER (
                    PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                    ORDER BY order_date
                ), 0),
        2)                                                          AS revenue_wow_growth_pct,

        ROUND(
            (total_bookings_count
             - LAG(total_bookings_count, 7) OVER (
                   PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                   ORDER BY order_date
               )
            ) * 100.0
            / NULLIF(
                LAG(total_bookings_count, 7) OVER (
                    PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                    ORDER BY order_date
                ), 0),
        2)                                                          AS volume_wow_growth_pct,

        LAG(net_income_usd, 30) OVER (
            PARTITION BY customer_value_segment, cuisine_type, merchant_tier
            ORDER BY order_date
        )                                                           AS net_income_usd_prior_month,

        ROUND(
            (net_income_usd
             - LAG(net_income_usd, 30) OVER (
                   PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                   ORDER BY order_date
               )
            ) * 100.0
            / NULLIF(
                LAG(net_income_usd, 30) OVER (
                    PARTITION BY customer_value_segment, cuisine_type, merchant_tier
                    ORDER BY order_date
                ), 0),
        2)                                                          AS income_mom_growth_pct

    FROM daily_summary
)


-- ============================================================
-- STEP 8: Final output
-- ============================================================
SELECT
    order_date,
    order_week,
    order_month,
    customer_value_segment,
    customer_lifecycle,
    customer_city,
    customer_state,
    restaurant_name,
    cuisine_type,
    merchant_tier,
    restaurant_city,
    latest_payment_method,
    total_bookings_count,
    successful_orders_count,
    cancelled_orders_count,
    failed_orders_count,
    gross_bookings_usd,
    gross_revenue_usd,
    net_revenue_usd,
    net_income_usd,
    avg_order_value_usd,
    avg_subtotal_usd,
    platform_commission_usd,
    platform_delivery_fees_usd,
    total_refunds_usd,
    total_marketing_discounts_usd,
    total_tips_pass_through_usd,
    order_success_rate_pct,
    cancellation_rate_pct,
    failure_rate_pct,
    refund_rate_pct,
    payment_match_rate_pct,
    effective_commission_rate_pct,
    gross_revenue_usd_prior_week,
    revenue_wow_growth_pct,
    volume_wow_growth_pct,
    net_income_usd_prior_month,
    income_mom_growth_pct
FROM period_comparison
ORDER BY order_date DESC, gross_revenue_usd DESC
