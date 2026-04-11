-- ============================================================
-- GOLD LAYER — User Dimension (dim_users)
-- Purpose: Unified identity lookup with optimized integer Surrogate Keys
-- ============================================================

WITH silver_users AS (
    SELECT * FROM {{ ref('users') }}
),

final_users AS (
    SELECT
        -- Surrogate Key (Optimized Integer)
        ABS(HASH(user_id))                                  AS user_sk,

        -- IDs & Identity (Natural ID preserved)
        user_id,
        first_name,
        last_name,
        email,
        phone,

        -- Location
        city,
        state,
        zip_code,
        country,

        -- Lifecycle
        registered_date                                     AS registered_at,
        last_login_at,
        account_status,
        user_role                                           AS platform_role,

        -- System Settings
        is_email_verified,
        preferred_lang                                      AS preferred_language,
        has_notification_opt_in                             AS is_opted_in_notifications,

        -- Referral & Loyalty
        referral_code,
        referred_by,
        loyalty_tier,
        total_spent_usd                                     AS lifetime_spend_total_usd,

        -- Audit info
        _loaded_at

    FROM silver_users
)

SELECT * FROM final_users
