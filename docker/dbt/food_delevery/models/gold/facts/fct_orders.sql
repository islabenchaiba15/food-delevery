-- ============================================================
-- GOLD LAYER — Order Fact (fct_orders)
-- Purpose: Unified order transactions (Financial & Status)
-- ============================================================

WITH silver_orders AS (
    SELECT * FROM {{ ref('orders') }}
),

final_orders AS (
    SELECT
        -- IDs (Foreign Keys)
        order_id,
        customer_id,
        restaurant_id,
        driver_id,

        -- Timestamps
        order_at,
        order_date,
        order_time,

        -- Status & Type
        status                                            AS order_status,
        channel                                           AS order_channel,
        order_type,
        is_reorder_flag,
        
        -- Business KPIs
        items_count,
        subtotal_amount,
        tax_amount,
        delivery_fee_amount,
        discount_amount,
        tip_amount,
        total_paid_amount,
        refund_amount,

        -- Interaction
        promo_code,
        customer_rating,
        notes,
        cancellation_reason,

        -- High-Level Flags
        (status = 'Delivered')                            AS is_completed,
        (status = 'Cancelled')                            AS is_cancelled,
        (customer_rating >= 4)                            AS is_high_rated,
        (customer_rating <= 2)                            AS is_low_rated

    FROM silver_orders
)

SELECT * FROM final_orders
