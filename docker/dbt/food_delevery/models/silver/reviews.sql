-- ============================================================
-- SILVER LAYER — Reviews Staging
-- Source: FOOD_DELIVERY_DW.BRONZE.REVIEWS
-- Purpose: Clean, normalize, and categorize customer reviews
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('food_delivery_raw', 'REVIEWS') }}
),

transformed AS (
    SELECT
        -- IDs
        TRIM(review_id)                                     AS review_id,
        TRIM(order_id)                                      AS order_id,
        TRIM(customer_id)                                   AS customer_id,
        TRIM(restaurant_id)                                 AS restaurant_id,
        TRIM(driver_id)                                     AS driver_id,

        -- Ratings (Standardize missing ratings as NULL)
        TRY_CAST(CAST(overall_rating AS VARCHAR) AS NUMERIC(3, 1))           AS rating_overall,
        TRY_CAST(CAST(food_rating AS VARCHAR) AS NUMERIC(3, 1))              AS rating_food,
        TRY_CAST(CAST(delivery_rating AS VARCHAR) AS NUMERIC(3, 1))          AS rating_delivery,
        TRY_CAST(CAST(driver_rating AS VARCHAR) AS NUMERIC(3, 1))            AS rating_driver,
        TRY_CAST(CAST(packaging_rating AS VARCHAR) AS NUMERIC(3, 1))         AS rating_packaging,
        TRY_CAST(CAST(value_rating AS VARCHAR) AS NUMERIC(3, 1))             AS rating_value,

        -- Content & Sentiment
        TRIM(review_text)                                   AS review_content,
        TRY_CAST(CAST(review_date AS VARCHAR) AS DATE)                       AS review_date,
        INITCAP(TRIM(sentiment))                            AS sentiment,
        
        -- Verification & Interaction
        TRY_CAST(CAST(is_verified AS VARCHAR) AS BOOLEAN)                    AS is_verified_purchase,
        COALESCE(helpful_votes, 0)                           AS helpful_votes_count,
        COALESCE(report_count, 0)                            AS report_count,
        
        -- Response logic
        COALESCE(TRIM(restaurant_response), 'No Response') AS restaurant_response_text,
        (restaurant_response IS NOT NULL AND TRIM(restaurant_response) != '') AS has_restaurant_responded,

        -- Metadata
        TRY_CAST(CAST(is_featured AS VARCHAR) AS BOOLEAN)                    AS is_featured_review,
        INITCAP(TRIM(platform))                             AS source_platform,
        TRY_CAST(CAST(images_attached AS VARCHAR) AS BOOLEAN)                AS has_images,

        -- Audit Metadata
        CAST(event_time AS TIMESTAMP)                        AS _extracted_at,
        CURRENT_TIMESTAMP()                                   AS _loaded_at

    FROM source
    WHERE review_id IS NOT NULL
)

SELECT
    *,
    -- Derived flags
    (rating_overall >= 4)                                   AS is_positive_review,
    (rating_overall <= 2)                                   AS is_negative_review,
    (sentiment = 'Negative' AND rating_overall >= 4)        AS is_conflicting_sentiment  -- Possible bot/outlier
FROM transformed
-- Deduplicate: keep only the most recent extract for each review_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY _extracted_at DESC) = 1