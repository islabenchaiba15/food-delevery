-- ============================================================
-- GOLD LAYER — Payment Fact (fct_payments)
-- Purpose: Financial transaction ledger with optimized integer Surrogate Keys
-- ============================================================

WITH silver_payments AS (
    SELECT * FROM {{ ref('payments') }}
),

final_payments AS (
    SELECT
        -- Surrogate Keys (Optimized Integer)
        ABS(HASH(payment_id))                               AS payment_sk,
        ABS(HASH(order_id))                                 AS order_sk,
        ABS(HASH(customer_id))                              AS customer_sk,
        transaction_id,

        -- Timestamps
        payment_date,
        payment_time,
        payment_at_ts,

        -- Payment Details
        payment_method,
        payment_gateway,
        currency,

        -- Financials (Cleaned)
        amount_usd,
        fee_amount_usd                                      AS gateway_fee_usd,
        net_amount_usd,
        refund_amount_usd                                   AS refund_amount,

        -- Status & Performance
        status                                              AS payment_status,
        is_refunded,
        fraud_score,
        fraud_risk_level,

        -- Metadata
        ip_country,
        billing_zip_code,

        -- High-Level Flags
        (status = 'Success')                                AS is_fully_paid,
        (status = 'Failed')                                 AS is_failed_transaction,
        (fraud_risk_level = 'High')                         AS is_high_risk_transaction

    FROM silver_payments
)

SELECT * FROM final_payments
