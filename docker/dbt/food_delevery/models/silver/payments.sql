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
        TRY_CAST(payment_date AS TIMESTAMP)                  AS payment_at,
        DATE_TRUNC('month', TRY_CAST(payment_date AS TIMESTAMP)) AS payment_month,
        DAYNAME(TRY_CAST(payment_date AS TIMESTAMP))         AS payment_day_of_week,
        HOUR(TRY_CAST(payment_date AS TIMESTAMP))            AS payment_hour,

        -- Payment Details
        INITCAP(TRIM(payment_method))                        AS payment_method,
        INITCAP(TRIM(payment_gateway))                       AS payment_gateway,
        UPPER(TRIM(currency))                                AS currency,

        -- Financials
        -- Handle negative amounts (0.13% found) by taking absolute value
        CAST(ABS(amount) AS NUMERIC(18, 2))                  AS amount_abs,
        CAST(amount AS NUMERIC(18, 2))                       AS amount_raw,
        CAST(tax_amount AS NUMERIC(18, 2))                   AS tax_amount,
        CAST(fee_amount AS NUMERIC(18, 2))                   AS fee_amount,
        
        -- Net amount after fees and tax
        CAST(ABS(amount) - tax_amount - fee_amount AS NUMERIC(18, 2)) AS net_amount,

        -- Status & Auth
        INITCAP(TRIM(status))                                AS status,
        TRIM(auth_code)                                      AS auth_code,
        TRIM(transaction_id)                                 AS transaction_id,

        -- Refund logic (5% of payments)
        CAST(COALESCE(refund_amount, 0) AS NUMERIC(18, 2))   AS refund_amount,
        (refund_amount > 0)                                  AS is_refunded,

        -- Fraud & Risk
        CAST(COALESCE(fraud_score, 0) AS FLOAT)              AS fraud_score,
        CASE
            WHEN fraud_score >= 0.8 THEN 'High'
            WHEN fraud_score >= 0.5 THEN 'Medium'
            ELSE 'Low'
        END                                                  AS fraud_risk_level,

        -- Metadata
        TRIM(ip_address)                                     AS ip_address,
        INITCAP(TRIM(device_type))                           AS device_type,

        -- Data Quality Flags
        (amount < 0)                                         AS dq_is_negative_amount,
        (status = 'Success' AND auth_code IS NULL)           AS dq_success_missing_auth,

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