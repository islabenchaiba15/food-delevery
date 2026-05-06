-- ============================================================
-- GOLD LAYER — Billing Fact (fct_billing)
-- Purpose: Invoicing & merchant payment monitoring with optimized integer Surrogate Keys
-- ============================================================

WITH silver_billing AS (
    SELECT * FROM {{ ref('billing') }}
),
///////////
final_billing AS (
    SELECT
        -- Surrogate Keys (Optimized Integer)
        ABS(HASH(invoice_id))                               AS billing_sk,
        ABS(HASH(restaurant_id))                            AS restaurant_sk,

        -- Natural IDs (Kept for traceability)
        transaction_ref,

        -- Timestamps
        invoice_date,
        due_date,
        payment_date,
        
        -- Durations
        days_to_due,
        days_to_payment,

        -- Status
        status                                              AS billing_status,
        billing_period,
        plan_tier,
        is_overdue,

        -- Financials (Corrected in Silver)
        total_usd                                           AS invoice_total_usd,
        base_fee_usd,
        usage_orders,
        overage_fee_usd,
        addon_fee_usd,
        discount_usd,
        tax_usd,
        
        -- Reconciliation info
        total_usd_original                                  AS raw_source_total_usd,
        total_usd_variance                                  AS calculation_variance_usd,

        -- Payment Meta
        payment_method,
        payment_gateway,
        currency,
        
        -- Audit details
        notes,
        created_by

    FROM silver_billing
)

SELECT * FROM final_billing
