-- ============================================================
-- SILVER LAYER — Payments Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.PAYMENTS
-- Purpose: Clean, normalize, and validate payment transactions
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'PAYMENTS') }}
),

transformed AS (
    SELECT
        -- IDs
        TRIM(payment_id)                                     AS payment_id,
        TRIM(order_id)                                       AS order_id,
        TRIM(customer_id)                                    AS customer_id,

        -- Timestamps
        TRY_CAST(payment_date AS DATE)                       AS payment_date,
        TRY_CAST(payment_time AS TIME)                       AS payment_time,
        
        -- Combine to timestamp
        TIMESTAMP_FROM_PARTS(
            TRY_CAST(payment_date AS DATE), 
            TRY_CAST(payment_time AS TIME)
        )                                                    AS payment_at_ts,

        -- Payment Details
        INITCAP(TRIM(payment_method))                        AS payment_method,
        INITCAP(TRIM(gateway))                               AS payment_gateway,
        UPPER(TRIM(currency))                                AS currency,

        -- Financials
        -- Handle negative amounts (0.13% found) by taking absolute value
        CAST(ABS(amount_usd) AS NUMERIC(18, 2))              AS amount_usd_abs,
        CAST(amount_usd AS NUMERIC(18, 2))                   AS amount_usd_raw,
        CAST(gateway_fee_usd AS NUMERIC(18, 2))              AS fee_amount_usd,
        
        -- Net amount after fees
        CAST(ABS(amount_usd) - COALESCE(gateway_fee_usd, 0) AS NUMERIC(18, 2)) AS net_amount_usd,

        -- Status & Auth
        INITCAP(TRIM(status))                                AS status,
        TRIM(transaction_id)                                 AS transaction_id,

        -- Refund logic
        CAST(COALESCE(refund_amount_usd, 0) AS NUMERIC(18, 2)) AS refund_amount_usd,
        CAST(is_refunded AS BOOLEAN)                         AS is_refunded_flag,

        -- Fraud & Risk
        CAST(COALESCE(fraud_score, 0) AS FLOAT)              AS fraud_score,
        CASE
            WHEN fraud_score >= 0.8 THEN 'High'
            WHEN fraud_score >= 0.5 THEN 'Medium'
            ELSE 'Low'
        END                                                  AS fraud_risk_level,

        -- Metadata
        UPPER(TRIM(ip_country))                              AS ip_country,
        TRIM(billing_zip)                                    AS billing_zip_code,

        -- Data Quality Flags
        (amount_usd < 0)                                     AS dq_is_negative_amount,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE payment_id IS NOT NULL
)

SELECT * 
FROM transformed
-- Deduplicate: keep only the most recent extract for each payment_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _extracted_at DESC) = 1