WITH raw_users AS (
    SELECT * FROM {{ source('food_delivery_raw', 'USERS') }}
),

cleaned AS (
    SELECT
        user_id,
        first_name,
        last_name,
        LOWER(email) AS email,
        phone,
        city,
        state,
        zip_code,
        UPPER(country) AS country,
        CAST(registration_date AS DATE) AS registered_date,
        CAST(last_login AS TIMESTAMP) AS last_login_at,
        account_status,
        role AS user_role,
        CAST(email_verified AS BOOLEAN) AS is_email_verified,
        preferred_lang,
        referral_code,
        referred_by,
        CAST(notification_opt AS BOOLEAN) AS has_notification_opt_in,
        loyalty_tier,
        CAST(total_spent_usd AS NUMERIC(18, 2)) AS total_spent_usd,
        -- Audit columns
        CAST(event_time AS TIMESTAMP) AS _loaded_at
    FROM raw_users
    WHERE user_id IS NOT NULL
)

SELECT * FROM cleaned
-- Deduplicate by taking the latest event_time for each user_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY _loaded_at DESC) = 1