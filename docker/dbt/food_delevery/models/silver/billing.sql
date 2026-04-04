-- ============================================================
-- SILVER LAYER — Billing Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.BILLING
-- Purpose: Clean, validate, and correct billing/invoice data
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'BILLING') }}
),

base_cleaning AS (
    SELECT
        -- IDs
        TRIM(invoice_id)                                                        AS invoice_id,
        TRIM(restaurant_id)                                                     AS restaurant_id,

        -- ── DATES (Initial cleaning and Swap Logic) ──────────────────────────
        TRY_CAST(CAST(invoice_date AS VARCHAR) AS DATE)                         AS raw_invoice_date,
        TRY_CAST(CAST(due_date AS VARCHAR) AS DATE)                             AS raw_due_date,
        TRY_CAST(CAST(payment_date AS VARCHAR) AS DATE)                         AS raw_payment_date,

        -- ── FINANCIALS ───────────────────────────────────────────────────────
        COALESCE(base_fee_usd, 0)                                               AS base_fee_usd,
        COALESCE(usage_orders, 0)                                               AS usage_orders,
        COALESCE(overage_fee_usd, 0)                                            AS overage_fee_usd,
        COALESCE(addon_fee_usd, 0)                                              AS addon_fee_usd,
        COALESCE(discount_usd, 0)                                               AS discount_usd,
        COALESCE(tax_usd, 0)                                                    AS tax_usd,
        total_usd                                                               AS total_usd_original_raw,

        -- ── CATEGORICALS ─────────────────────────────────────────────────────
        INITCAP(TRIM(billing_period))                                           AS billing_period,
        INITCAP(TRIM(plan_tier))                                                AS plan_tier,
        INITCAP(TRIM(status))                                                   AS raw_status,
        INITCAP(TRIM(payment_method))                                           AS payment_method,
        INITCAP(TRIM(payment_gateway))                                          AS payment_gateway,
        UPPER(TRIM(currency))                                                   AS currency,

        -- ── FREE TEXT ────────────────────────────────────────────────────────
        TRIM(transaction_ref)                                                   AS transaction_ref,
        COALESCE(NULLIF(TRIM(notes), ''), 'None')                               AS notes,
        TRIM(created_by)                                                        AS created_by

    FROM source
    WHERE invoice_id IS NOT NULL
),

standardized AS (
    SELECT
        *,
        -- Fix date swaps (if due_date is before invoice_date)
        CASE 
            WHEN raw_due_date < raw_invoice_date THEN raw_due_date 
            ELSE raw_invoice_date 
        END                                                                     AS invoice_date,
        CASE 
            WHEN raw_due_date < raw_invoice_date THEN raw_invoice_date 
            ELSE raw_due_date 
        END                                                                     AS due_date,

        -- Recomputed Ledger Total
        ROUND(base_fee_usd + overage_fee_usd + addon_fee_usd - discount_usd + tax_usd, 2) 
                                                                                AS total_usd_calculated
    FROM base_cleaning
),

final_cleaning AS (
    SELECT
        *,
        -- payment_date logic (use corrected invoice_date)
        CASE
            WHEN raw_payment_date < invoice_date THEN NULL                          -- impossible, discard
            WHEN raw_payment_date IS NULL AND raw_status = 'Paid' THEN invoice_date -- Paid but no date -> fallback
            ELSE raw_payment_date
        END                                                                     AS payment_date,

        -- status logic
        CASE
            WHEN raw_payment_date IS NOT NULL AND raw_payment_date >= invoice_date AND raw_status != 'Paid'
                THEN 'Paid'
            WHEN raw_status = 'Paid' AND raw_payment_date IS NULL
                THEN 'Paid - Date Unknown'
            ELSE raw_status
        END                                                                     AS status
    FROM standardized
)

SELECT
    invoice_id,
    restaurant_id,
    invoice_date,
    due_date,
    payment_date,
    
    -- Corrected DATEDIFF for Snowflake
    DATEDIFF('day', invoice_date, due_date)                                   AS days_to_due,
    DATEDIFF('day', invoice_date, payment_date)                               AS days_to_payment,
    
    CASE
        WHEN status IN ('Pending', 'Partial') AND due_date < CURRENT_DATE()
        THEN TRUE ELSE FALSE
    END                                                                       AS is_overdue,

    billing_period,
    plan_tier,
    status,
    payment_method,
    payment_gateway,
    currency,
    
    base_fee_usd,
    usage_orders,
    overage_fee_usd,
    addon_fee_usd,
    discount_usd,
    tax_usd,
    total_usd_calculated                                                     AS total_usd,
    ROUND(total_usd_original_raw, 2)                                         AS total_usd_original,
    ROUND(total_usd_original_raw - total_usd_calculated, 2)                  AS total_usd_variance,

    -- Audit Flags
    (raw_due_date < raw_invoice_date)                                        AS flag_due_before_invoice_fixed,
    (raw_payment_date IS NOT NULL AND raw_payment_date < raw_invoice_date)   AS flag_payment_before_invoice_nulled,
    (raw_status = 'Paid' AND raw_payment_date IS NULL)                       AS flag_paid_missing_date_corrected,
    (ABS(total_usd_original_raw - total_usd_calculated) > 0.02)              AS flag_total_mismatch,

    transaction_ref,
    notes,
    created_by,
    CURRENT_TIMESTAMP()                                                      AS _loaded_at

FROM final_cleaning
-- Deduplicate: keep the latest record if duplicates exist
QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY _loaded_at DESC) = 1