-- ============================================================
-- GOLD LAYER — Order Fact (fct_orders)
-- Purpose: Unified order transactions with optimized integer Surrogate Keys
-- ============================================================

WITH silver_orders AS (
    SELECT * FROM {{ ref('orders') }}
),

final_orders AS (
    SELECT
        -- Surrogate Keys (Optimized Integer)
        ABS(HASH(order_id))                                 AS order_sk,
        ABS(HASH(customer_id))                              AS customer_sk,
        ABS(HASH(restaurant_id))                            AS restaurant_sk,
        ABS(HASH(driver_id))                                AS driver_sk,

        order_at,
        order_date,
        order_time,

        -- Status & Type
        status                                            AS order_status,
        channel                                           AS order_channel,
        order_type,
        is_reorder                                        AS is_reorder_flag,
        
        -- Business KPIs
        items_count,
        subtotal_usd                                      AS subtotal_amount,
        tax_usd                                           AS tax_amount,
        delivery_fee_usd                                  AS delivery_fee_amount,
        discount_usd                                      AS discount_amount,
        tip_usd                                           AS tip_amount,
        total_usd                                         AS total_paid_amount,
        refund_amount_usd                                 AS refund_amount,

        -- Interaction
        promo_code,
        customer_rating,
        notes,
        cancellation_reason,

        -- High-Level Flags

    FROM silver_orders
)

SELECT * FROM final_orders
