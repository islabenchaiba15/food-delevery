-- ============================================================
-- SILVER LAYER — Payments Staging (Clean & Opinionated)
-- Source: FOOD_DELIVERY_DW.BRONZE.PAYMENTS
-- Purpose: Clean, normalize, and repair payment transactions
--          using best-practice imputation. Every field
--          delivered is production-ready.
--
-- Fix strategy summary:
--   1.  Negative amounts           → ABS()
--   2.  Refund flag/amount mismatch → derive both from each other
--   3.  Refund date before payment  → NULL the refund date
--   4.  Failed/Pending + refund    → NULL refund fields
--   5.  Refund amount > original   → cap at original amount
--   6.  Digital wallet + card type → NULL card_type
--   7.  Duplicate transaction IDs  → keep earliest payment_date
--   8.  Gateway fee > amount       → cap fee at amount, net = 0
--   9.  Fraud flag/score mismatch  → derive flag from score
--   10. Duplicate payment IDs      → keep latest _extracted_at
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'PAYMENTS') }}
),

-- ----------------------------------------------------------------
-- STEP 1 — Base casting & atomic fixes
--           Applied first so every subsequent CTE works on
--           clean types and non-negative amounts.
-- ----------------------------------------------------------------
casted AS (
    SELECT
        TRIM(payment_id)                                AS payment_id,
        TRIM(order_id)                                  AS order_id,
        TRIM(customer_id)                               AS customer_id,
        INITCAP(TRIM(payment_method))                   AS payment_method,
        INITCAP(TRIM(card_type))                        AS card_type_raw,
        card_last4,
        UPPER(TRIM(currency))                           AS currency,
        TRY_CAST(payment_date AS DATE)                  AS payment_date,
        TRY_CAST(payment_time AS TIME)                  AS payment_time,
        INITCAP(TRIM(status))                           AS status,
        TRIM(transaction_id)                            AS transaction_id,
        INITCAP(TRIM(gateway))                          AS payment_gateway,

        -- Fix 1: amounts are always non-negative
        CAST(ABS(amount_usd) AS NUMERIC(18, 2))         AS amount_usd,
        CAST(ABS(COALESCE(gateway_fee_usd, 0))
             AS NUMERIC(18, 2))                         AS gateway_fee_usd,

        CAST(is_refunded AS BOOLEAN)                    AS is_refunded_raw,
        CAST(COALESCE(refund_amount_usd, 0)
             AS NUMERIC(18, 2))                         AS refund_amount_usd_raw,
        TRY_CAST(refund_date AS DATE)                   AS refund_date_raw,

        CAST(COALESCE(fraud_score, 0) AS FLOAT)         AS fraud_score,
        CAST(is_flagged AS BOOLEAN)                     AS is_flagged_raw,
        UPPER(TRIM(ip_country))                         AS ip_country,
        TRIM(billing_zip)                               AS billing_zip_code,
        CAST(event_time AS TIMESTAMP)                   AS _extracted_at
    FROM source
    WHERE payment_id IS NOT NULL
),

-- ----------------------------------------------------------------
-- STEP 2 — Status-aware refund nullification (Fix 4)
--           A Failed or Pending payment was never collected,
--           so any refund data on it is noise — wipe it.
-- ----------------------------------------------------------------
status_fixed AS (
    SELECT
        *,
        CASE
            WHEN status IN ('Failed', 'Pending') THEN FALSE
            ELSE is_refunded_raw
        END                                             AS is_refunded_s,

        CASE
            WHEN status IN ('Failed', 'Pending') THEN 0
            ELSE refund_amount_usd_raw
        END                                             AS refund_amount_usd_s,

        CASE
            WHEN status IN ('Failed', 'Pending') THEN NULL
            ELSE refund_date_raw
        END                                             AS refund_date_s
    FROM casted
),

-- ----------------------------------------------------------------
-- STEP 3 — Temporal repair (Fix 3)
--           A refund date that precedes the payment is impossible.
--           Best practice: NULL it — do not guess the correct date.
-- ----------------------------------------------------------------
date_fixed AS (
    SELECT
        *,
        CASE
            WHEN refund_date_s < payment_date THEN NULL
            ELSE refund_date_s
        END                                             AS refund_date_clean
    FROM status_fixed
),

-- ----------------------------------------------------------------
-- STEP 4 — Refund amount / flag reconciliation (Fix 2 & 5)
--
--   Priority rules:
--     a. If refund_amount > original → cap at original amount
--     b. If is_refunded=TRUE but amount=0 → assume full refund,
--        set amount = original (amount not recorded, not absent)
--     c. If is_refunded=FALSE but amount>0 → amount wins,
--        set flag to TRUE (amount is the stronger evidence)
--     d. Re-derive is_refunded from the cleaned amount
-- ----------------------------------------------------------------
refund_fixed AS (
    SELECT
        *,

        CASE
            -- Fix 5: cap refund at the original charge
            WHEN refund_amount_usd_s > amount_usd
                THEN amount_usd
            -- Fix 2b: flag=TRUE but amount missing → full refund assumed
            WHEN is_refunded_s = TRUE AND refund_amount_usd_s = 0
                THEN amount_usd
            -- Fix 2c: amount present, flag wrong → trust the amount
            ELSE refund_amount_usd_s
        END                                             AS refund_amount_usd,

        -- Re-derive the flag: TRUE iff a positive refund exists after cleaning
        CASE
            WHEN refund_amount_usd_s > 0
              OR (is_refunded_s = TRUE AND refund_amount_usd_s = 0)
                THEN TRUE
            ELSE FALSE
        END                                             AS is_refunded
    FROM date_fixed
),

-- ----------------------------------------------------------------
-- STEP 5 — Fee repair (Fix 8)
--           A gateway fee cannot exceed the transaction amount.
--           Cap the fee; net revenue floors at 0.
-- ----------------------------------------------------------------
fee_fixed AS (
    SELECT
        *,
        LEAST(gateway_fee_usd, amount_usd)              AS fee_amount_usd,
        GREATEST(
            CAST(
                amount_usd - LEAST(gateway_fee_usd, amount_usd)
                AS NUMERIC(18, 2)
            ),
            0
        )                                               AS net_amount_usd
    FROM refund_fixed
),

-- ----------------------------------------------------------------
-- STEP 6 — Card type repair (Fix 6)
--           PayPal, Apple Pay, Google Pay, and Gift Cards are
--           wallet-based — they carry no card network type.
-- ----------------------------------------------------------------
card_fixed AS (
    SELECT
        *,
        CASE
            WHEN payment_method IN (
                'Paypal', 'Apple Pay', 'Google Pay', 'Gift Card'
            ) THEN NULL
            ELSE card_type_raw
        END                                             AS card_type
    FROM fee_fixed
),

-- ----------------------------------------------------------------
-- STEP 7 — Fraud flag reconciliation (Fix 9)
--           fraud_score is a continuous model output and is more
--           reliable than the boolean flag. Derive is_flagged
--           deterministically from the score.
--             >= 0.8 → High  → flagged
--             >= 0.5 → Medium → not flagged (monitored via level)
--             <  0.5 → Low   → not flagged
-- ----------------------------------------------------------------
fraud_fixed AS (
    SELECT
        *,
        CASE
            WHEN fraud_score >= 0.8 THEN 'High'
            WHEN fraud_score >= 0.5 THEN 'Medium'
            ELSE 'Low'
        END                                             AS fraud_risk_level,
        CASE WHEN fraud_score >= 0.8 THEN TRUE ELSE FALSE END
                                                        AS is_flagged
    FROM card_fixed
),

-- ----------------------------------------------------------------
-- STEP 8 — Duplicate transaction_id resolution (Fix 7)
--           The same transaction_id on two payment_ids = double
--           entry. Keep the earliest payment_date as the original.
-- ----------------------------------------------------------------
txn_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY payment_date ASC, _extracted_at ASC
        )                                               AS _txn_rn
    FROM fraud_fixed
),

-- ----------------------------------------------------------------
-- STEP 9 — Payment_id deduplication (Fix 10)
--           Same payment_id re-extracted → keep the freshest row.
-- ----------------------------------------------------------------
pay_deduped AS (
    SELECT *
    FROM txn_deduped
    WHERE _txn_rn = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY payment_id
        ORDER BY _extracted_at DESC
    ) = 1
)

-- ----------------------------------------------------------------
-- FINAL — Clean projection only. No intermediate columns exposed.
-- ----------------------------------------------------------------
SELECT
    -- Keys
    payment_id,
    order_id,
    customer_id,

    -- Timestamps
    payment_date,
    payment_time,
    TIMESTAMP_FROM_PARTS(payment_date, payment_time)    AS payment_at_ts,
    refund_date_clean                                   AS refund_date,

    -- Payment identifiers
    transaction_id,
    payment_method,
    card_type,
    card_last4,
    currency,
    payment_gateway,
    status,

    -- Financials (all non-negative, internally consistent)
    amount_usd,
    fee_amount_usd,
    net_amount_usd,
    refund_amount_usd,
    is_refunded,

    -- Risk
    fraud_score,
    fraud_risk_level,
    is_flagged,

    -- Geo
    billing_zip_code,
    ip_country,

    -- Audit
    _extracted_at,
    CURRENT_TIMESTAMP()                                 AS _loaded_at

FROM pay_deduped