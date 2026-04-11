-- ============================================================
-- KPI MART — Subscription & Billing Summary
-- Purpose: Executive finance dashboard for recurring revenue (subscriptions)
--          from restaurants, tracking paid vs overdue invoices.
-- Grain: One row per (invoice_date × plan_tier × restaurant)
-- ============================================================

WITH billing AS (
    SELECT * FROM {{ ref('fct_billing') }}
),

restaurants AS (
    SELECT
        restaurant_sk,
        restaurant_name,
        cuisine_type,
        city AS restaurant_city
    FROM {{ ref('dim_restaurants') }}
),

enriched_billing AS (
    SELECT
        b.invoice_date,
        DATE_TRUNC('week', b.invoice_date) AS invoice_week,
        DATE_TRUNC('month', b.invoice_date) AS invoice_month,
        
        -- Dimensions
        r.restaurant_name,
        r.cuisine_type,
        r.restaurant_city,
        b.plan_tier,
        b.billing_period,
        b.billing_status,
        b.payment_method,

        -- Standardized Revenue Status
        CASE 
            WHEN b.billing_status IN ('Paid', 'Paid - Date Unknown') THEN 'Collected'
            WHEN b.is_overdue THEN 'Overdue'
            ELSE 'Pending'
        END AS revenue_status,

        -- Financials
        b.invoice_total_usd,
        b.base_fee_usd,
        b.overage_fee_usd,
        b.addon_fee_usd,
        b.discount_usd,
        b.tax_usd

    FROM billing b
    LEFT JOIN restaurants r ON b.restaurant_sk = r.restaurant_sk
),

daily_subscription_summary AS (
    SELECT
        invoice_date,
        invoice_week,
        invoice_month,

        -- Dimensions
        restaurant_name,
        cuisine_type,
        restaurant_city,
        plan_tier,
        billing_period,

        -- 1. VOLUME METRICS
        COUNT(*) AS total_invoices_count,
        SUM(CASE WHEN revenue_status = 'Collected' THEN 1 ELSE 0 END) AS collected_invoices_count,
        SUM(CASE WHEN revenue_status = 'Overdue' THEN 1 ELSE 0 END) AS overdue_invoices_count,

        -- 2. REVENUE METRICS (Total Billed)
        SUM(invoice_total_usd) AS gross_billed_usd,
        SUM(base_fee_usd) AS billed_base_fees_usd,
        SUM(overage_fee_usd + addon_fee_usd) AS billed_usage_fees_usd,
        SUM(discount_usd) AS total_discounts_given_usd,

        -- 3. COLLECTED REVENUE (Realized / Paid)
        SUM(CASE WHEN revenue_status = 'Collected' THEN invoice_total_usd ELSE 0 END) AS recognized_recurring_revenue_usd,
        SUM(CASE WHEN revenue_status = 'Collected' THEN base_fee_usd ELSE 0 END) AS collected_base_fees_usd,
        SUM(CASE WHEN revenue_status = 'Collected' THEN (overage_fee_usd + addon_fee_usd) ELSE 0 END) AS collected_usage_fees_usd,

        -- 4. AT RISK REVENUE (Overdue)
        SUM(CASE WHEN revenue_status = 'Overdue' THEN invoice_total_usd ELSE 0 END) AS overdue_uncollected_revenue_usd

    FROM enriched_billing
    GROUP BY
        invoice_date,
        invoice_week,
        invoice_month,
        restaurant_name,
        cuisine_type,
        restaurant_city,
        plan_tier,
        billing_period
)

SELECT *
FROM daily_subscription_summary
ORDER BY invoice_date DESC, recognized_recurring_revenue_usd DESC
