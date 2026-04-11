-- ============================================================
-- SILVER LAYER — Orders Staging (Clean & Opinionated)
-- Source: FOOD_DELIVERY_DW.BRONZE.ORDERS
-- Purpose: Clean, normalize, and repair order records
--          using best-practice imputation.
--
-- Fix strategy summary:
--   1.  Negative total_usd            → ABS()
--   2.  total_usd recomputed          → subtotal + tax + fee - discount
--                                       (tip is driver-direct, excluded)
--   3.  actual_delivery < prep_time   → NULL actual_delivery_min
--   4.  Rating on non-delivered orders→ NULL customer_rating
--   5.  Refund = 0 on refundable status → set to total_usd (full refund)
--   6.  Refund > total_usd            → cap at total_usd
--   7.  Pickup + delivery_fee > 0     → set delivery_fee to 0
--   8.  Pickup + driver_id            → NULL driver_id
--   9.  Promo code with discount = 0  → NULL promo_code
--   10. Discount > 0 with no promo    → set promo_code to 'UNKNOWN'
--   11. Cancellation reason on non-cancelled → NULL it
--       Missing reason on cancelled   → 'Unknown'
--   12. Duplicate order IDs           → keep latest event_time
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'ORDERS') }}
),

-- ----------------------------------------------------------------
-- STEP 1 — Base casting
--           Clean types and trim strings before any logic runs.
-- ----------------------------------------------------------------
casted AS (
    SELECT
        TRIM(order_id)                                          AS order_id,
        TRIM(customer_id)                                       AS customer_id,
        TRIM(restaurant_id)                                     AS restaurant_id,
        TRIM(driver_id)                                         AS driver_id,
        TRY_CAST(order_date AS DATE)                            AS order_date,
        TRY_CAST(order_time AS TIME)                            AS order_time,
        INITCAP(TRIM(status))                                   AS status,
        INITCAP(TRIM(channel))                                  AS channel,
        INITCAP(TRIM(order_type))                               AS order_type,

        -- Fix 1: amounts are always non-negative
        CAST(ABS(subtotal_usd)       AS NUMERIC(18, 2))         AS subtotal_usd,
        CAST(ABS(tax_usd)            AS NUMERIC(18, 2))         AS tax_usd,
        CAST(ABS(delivery_fee_usd)   AS NUMERIC(18, 2))         AS delivery_fee_usd,
        CAST(ABS(discount_usd)       AS NUMERIC(18, 2))         AS discount_usd,
        CAST(ABS(COALESCE(tip_usd,0))AS NUMERIC(18, 2))         AS tip_usd,
        CAST(ABS(total_usd)          AS NUMERIC(18, 2))         AS total_usd_raw,
        CAST(ABS(COALESCE(refund_amount_usd, 0))
                                     AS NUMERIC(18, 2))         AS refund_amount_usd_raw,

        CAST(items_count             AS INTEGER)                AS items_count,
        UPPER(TRIM(promo_code))                                 AS promo_code_raw,
        CAST(estimated_delivery_min  AS INTEGER)                AS estimated_delivery_min,
        CAST(actual_delivery_min     AS INTEGER)                AS actual_delivery_min_raw,
        CAST(prep_time_min           AS INTEGER)                AS prep_time_min,
        CAST(customer_rating         AS NUMERIC(3, 1))          AS customer_rating_raw,
        TRIM(notes)                                             AS notes,
        CAST(is_reorder              AS BOOLEAN)                AS is_reorder,
        TRIM(cancellation_reason)                               AS cancellation_reason_raw,
        CAST(event_time              AS TIMESTAMP)              AS _extracted_at
    FROM source
    WHERE order_id IS NOT NULL
),

-- ----------------------------------------------------------------
-- STEP 2 — Pickup-specific repairs (Fix 7 & 8)
--           Pickup orders are collected by the customer in person:
--           no courier is assigned and no delivery fee applies.
-- ----------------------------------------------------------------
pickup_fixed AS (
    SELECT
        *,
        CASE
            WHEN order_type = 'Pickup' THEN NULL
            ELSE driver_id
        END                                                     AS driver_id_clean,

        CASE
            WHEN order_type = 'Pickup' THEN 0
            ELSE delivery_fee_usd
        END                                                     AS delivery_fee_clean
    FROM casted
),

-- ----------------------------------------------------------------
-- STEP 3 — Recompute total from components (Fix 2)
--           tip_usd goes directly to the driver and is excluded.
--           Formula: subtotal + tax + delivery_fee - discount
--           Use the recomputed value; discard total_usd_raw.
-- ----------------------------------------------------------------
total_fixed AS (
    SELECT
        *,
        CAST(
            subtotal_usd
            + tax_usd
            + delivery_fee_clean
            - discount_usd
        AS NUMERIC(18, 2))                                      AS total_usd
    FROM pickup_fixed
),

-- ----------------------------------------------------------------
-- STEP 4 — Promo code / discount reconciliation (Fix 9 & 10)
--
--   a. Promo code present but discount = 0 → promo wasn't applied,
--      NULL the code so it doesn't pollute promo analysis.
--   b. Discount > 0 but no promo code → code wasn't captured,
--      set to 'UNKNOWN' so the discount isn't lost in aggregations.
-- ----------------------------------------------------------------
promo_fixed AS (
    SELECT
        *,
        CASE
            WHEN promo_code_raw IS NOT NULL AND discount_usd  = 0 THEN NULL
            WHEN promo_code_raw IS NULL     AND discount_usd  > 0 THEN 'UNKNOWN'
            ELSE promo_code_raw
        END                                                     AS promo_code
    FROM total_fixed
),

-- ----------------------------------------------------------------
-- STEP 5 — Refund repairs (Fix 5 & 6)
--
--   Refundable statuses: Refunded, Cancelled, Failed.
--   a. refund_amount = 0 on a refundable status → full refund assumed.
--   b. refund_amount > total_usd               → cap at total.
--   c. Non-refundable status with refund_amount → keep as-is
--      (partial refunds on delivered orders are legitimate).
-- ----------------------------------------------------------------
refund_fixed AS (
    SELECT
        *,
        CASE
            -- Refundable status with missing amount → full refund
            WHEN status IN ('Refunded', 'Cancelled', 'Failed')
                 AND refund_amount_usd_raw = 0
                THEN total_usd
            -- Cap refund at what was actually paid
            WHEN refund_amount_usd_raw > total_usd
                THEN total_usd
            ELSE refund_amount_usd_raw
        END                                                     AS refund_amount_usd
    FROM promo_fixed
),

-- ----------------------------------------------------------------
-- STEP 6 — Delivery time repair (Fix 3)
--           actual_delivery_min < prep_time_min is physically
--           impossible — the order cannot arrive before it is ready.
--           NULL the actual time; do not guess the real value.
-- ----------------------------------------------------------------
time_fixed AS (
    SELECT
        *,
        CASE
            WHEN actual_delivery_min_raw < prep_time_min THEN NULL
            ELSE actual_delivery_min_raw
        END                                                     AS actual_delivery_min
    FROM refund_fixed
),

-- ----------------------------------------------------------------
-- STEP 7 — Rating repair (Fix 4)
--           Only delivered orders can have a customer rating.
--           Statuses that never reach the customer: Cancelled,
--           Failed, Pending, Confirmed, Preparing, Ready.
-- ----------------------------------------------------------------
rating_fixed AS (
    SELECT
        *,
        CASE
            WHEN status NOT IN ('Delivered', 'Out For Delivery', 'Refunded')
                THEN NULL
            ELSE customer_rating_raw
        END                                                     AS customer_rating
    FROM time_fixed
),

-- ----------------------------------------------------------------
-- STEP 8 — Cancellation reason repair (Fix 11)
--
--   a. Non-cancelled order has a reason → NULL it (noise).
--   b. Cancelled order has no reason    → 'Unknown'.
-- ----------------------------------------------------------------
cancel_fixed AS (
    SELECT
        *,
        CASE
            WHEN status = 'Cancelled' AND cancellation_reason_raw IS NULL
                THEN 'Unknown'
            WHEN status != 'Cancelled'
                THEN NULL
            ELSE cancellation_reason_raw
        END                                                     AS cancellation_reason
    FROM rating_fixed
),

-- ----------------------------------------------------------------
-- STEP 9 — Deduplication (Fix 12)
--           Same order_id re-extracted → keep the freshest row.
-- ----------------------------------------------------------------
deduped AS (
    SELECT *
    FROM cancel_fixed
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id
        ORDER BY _extracted_at DESC
    ) = 1
)

-- ----------------------------------------------------------------
-- FINAL — Clean projection only. No intermediate columns exposed.
-- ----------------------------------------------------------------
SELECT
    -- Keys
    order_id,
    customer_id,
    restaurant_id,
    driver_id_clean                                             AS driver_id,

    -- Timestamps
    order_date,
    order_time,
    TIMESTAMP_FROM_PARTS(order_date, order_time)                AS order_at,

    -- Order attributes
    status,
    channel,
    order_type,
    is_reorder,
    promo_code,
    cancellation_reason,
    notes,

    -- Financials (all non-negative, internally consistent)
    subtotal_usd,
    tax_usd,
    delivery_fee_clean                                          AS delivery_fee_usd,
    discount_usd,
    total_usd,
    tip_usd,
    refund_amount_usd,

    -- Order size
    items_count,

    -- Delivery timing
    prep_time_min,
    estimated_delivery_min,
    actual_delivery_min,

    -- Customer feedback
    customer_rating,

    -- Audit
    _extracted_at,
    CURRENT_TIMESTAMP()                                         AS _loaded_at

FROM deduped