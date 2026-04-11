-- ============================================================
-- GOLD LAYER — Review Fact (fct_reviews)
-- Purpose: Customer feedback with optimized integer Surrogate Keys
-- ============================================================

WITH silver_reviews AS (
    SELECT * FROM {{ ref('reviews') }}
),

final_reviews AS (
    SELECT
        -- Surrogate Keys (Optimized Integer)
        ABS(HASH(review_id))                                AS review_sk,
        ABS(HASH(order_id))                                 AS order_sk,
        ABS(HASH(customer_id))                              AS customer_sk,
        ABS(HASH(restaurant_id))                            AS restaurant_sk,
        ABS(HASH(driver_id))                                AS driver_sk,

        -- Natural IDs (Kept for traceability)

        -- Timestamps
        review_date,

        -- Ratings (The "Fact" metrics)
        rating_overall,
        rating_food,
        rating_delivery,
        rating_driver,
        rating_packaging,
        rating_value,

        -- Content
        review_content,
        sentiment,

        -- Interaction & Engagement
        is_verified_purchase,
        helpful_votes_count,
        report_count,
        has_images,
        source_platform,

        -- Response Management
        has_restaurant_responded,
        restaurant_response_text,

        -- Business KPIs (Flags)
        is_featured_review,
        is_positive_review,
        is_negative_review,
        is_conflicting_sentiment                            AS is_potential_anomaly

    FROM silver_reviews
)

SELECT * FROM final_reviews
