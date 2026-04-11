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

        -- ── DATES ────────────────────────────────────────────────────────────
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

        -- Fix swapped dates: guarantee invoice_date <= due_date
        CASE
            WHEN raw_due_date < raw_invoice_date THEN raw_due_date
            ELSE raw_invoice_date
        END                                                                     AS invoice_date,
        CASE
            WHEN raw_due_date < raw_invoice_date THEN raw_invoice_date
            ELSE raw_due_date
        END                                                                     AS due_date,

        -- Recompute total from components; GREATEST(..., 0) prevents negative
        -- totals caused by a discount that exceeds the sum of all fee components
        GREATEST(
            ROUND(
                base_fee_usd + overage_fee_usd + addon_fee_usd
                - discount_usd + tax_usd,
                2
            ),
            0
        )                                                                       AS total_usd_calculated

    FROM base_cleaning
),

final_cleaning AS (
    SELECT
        *,

        -- ── PAYMENT DATE ─────────────────────────────────────────────────────
        CASE
            -- Payment predates corrected invoice date → impossible, discard
            WHEN raw_payment_date < invoice_date
                THEN NULL
            -- Paid in source but date is missing → fall back to invoice_date
            WHEN raw_payment_date IS NULL AND raw_status = 'Paid'
                THEN invoice_date
            ELSE raw_payment_date
        END                                                                     AS payment_date,

        -- ── STATUS ───────────────────────────────────────────────────────────
        CASE
            -- Has a valid payment date and was in a collectable status.
            -- Waived and Disputed are intentionally excluded: a payment date
            -- on those statuses does not mean the invoice was settled normally.
            WHEN raw_payment_date IS NOT NULL
                AND raw_payment_date >= invoice_date
                AND raw_status IN ('Pending', 'Partial', 'Overdue')
                THEN 'Paid'

            -- Source says Paid but no payment date could be recovered
            WHEN raw_status = 'Paid' AND raw_payment_date IS NULL
                THEN 'Paid - Date Unknown'

            -- Logically overdue: open invoice whose due date has passed
            WHEN raw_status IN ('Pending', 'Partial')
                AND due_date < CURRENT_DATE()
                THEN 'Overdue'

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

    DATEDIFF('day', invoice_date, due_date)                                     AS days_to_due,
    DATEDIFF('day', invoice_date, payment_date)                                 AS days_to_payment,

    -- Overdue flag: any non-terminal status whose due date has passed.
    -- Evaluates against the corrected status so it stays consistent with it.
    CASE
        WHEN status NOT IN ('Paid', 'Paid - Date Unknown', 'Waived', 'Disputed')
            AND due_date < CURRENT_DATE()
        THEN TRUE
        ELSE FALSE
    END                                                                         AS is_overdue,

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
    total_usd_calculated                                                        AS total_usd,
    ROUND(total_usd_original_raw, 2)                                            AS total_usd_original,
    ROUND(total_usd_original_raw - total_usd_calculated, 2)                     AS total_usd_variance,

    transaction_ref,
    notes,
    created_by,
    CURRENT_TIMESTAMP()                                                         AS _loaded_at

FROM final_cleaning

-- Deduplicate: keep the latest extraction. For exact clones created in the
-- same batch, Snowflake will pick one non-deterministically.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY invoice_id
    ORDER BY _loaded_at DESC
) = 1