# Silver Layer — Complete Table Schema Definitions

This document provides a detailed list of **all columns** for each of the 10 tables in the **Silver Layer**. These models are built using dbt and reside in the `SILVER` schema.

---

## 1. Users (`users`)
| Column Name | Business Logic / Description |
|:---|:---|
| `user_id` | Primary Key. |
| `first_name` | Original first name. |
| `last_name` | Original last name. |
| `email` | Standardized (lowercase). |
| `phone` | Original phone number. |
| `city` | User's city. |
| `state` | User's state. |
| `zip_code` | User's zip code. |
| `country` | Standardized (uppercase). |
| `registered_date` | Date of account creation (DATE). |
| `last_login_at` | Last login timestamp (TIMESTAMP). |
| `account_status` | Status (e.g., Active, Suspended). |
| `user_role` | Role on platform (User, Admin, etc.). |
| `is_email_verified` | Boolean flag. |
| `preferred_lang` | Language preference. |
| `referral_code` | User's unique referral code. |
| `referred_by` | Who referred this user. |
| `has_notification_opt_in`| Boolean flag. |
| `loyalty_tier` | Current membership tier. |
| `total_spent_usd` | Total lifetime spend (NUMERIC 18,2). |
| `_loaded_at` | Audit timestamp of processing. |

---

## 2. Orders (`orders`)
| Column Name | Business Logic / Description |
|:---|:---|
| `order_id` | Primary Key. |
| `customer_id` | Foreign Key. |
| `restaurant_id` | Foreign Key. |
| `driver_id` | Foreign Key. |
| `order_date` | Date of order. |
| `order_time` | Local time of order. |
| `order_at` | Combined order timestamp (TIMESTAMP). |
| `status` | Current state (Delivered, Cancelled, etc.). |
| `channel` | Source (Web, App, Phone). |
| `order_type` | Delivery or Pickup. |
| `subtotal_amount` | Amount before fees/tax. |
| `tax_amount` | Calculated tax. |
| `delivery_fee_amount` | Logistics fee. |
| `discount_amount` | Amount reduced via promos. |
| `total_paid_amount` | Final customer charge. |
| `items_count` | Number of items in order. |
| `promo_code` | Code applied to order. |
| `estimated_delivery_minutes`| Planned duration. |
| `actual_delivery_minutes` | Realized duration. |
| `prep_time_minutes` | Kitchen duration. |
| `customer_rating` | Numeric rating (1-5). |
| `notes` | Free-text instructions. |
| `is_reorder_flag` | Boolean identifier. |
| `tip_amount` | Gratuity paid. |
| `refund_amount` | Refunded portion. |
| `cancellation_reason` | Reason for failure. |
| `_loaded_at` | Audit timestamp. |

---

## 3. Payments (`payments`)
| Column Name | Business Logic / Description |
|:---|:---|
| `payment_id` | Primary Key. |
| `order_id` | Foreign Key. |
| `customer_id` | Foreign Key. |
| `payment_date` | Event date. |
| `payment_time` | Event time. |
| `payment_at_ts` | Combined timestamp. |
| `payment_method` | Card, PayPal, etc. |
| `payment_gateway` | Stripe, Adyen, etc. |
| `currency` | Standardized ISO (uppercase). |
| `amount_usd_abs` | Corrected absolute amount. |
| `amount_usd_raw` | Original recorded amount. |
| `fee_amount_usd` | Gateway processing fees. |
| `net_amount_usd` | Amount after gateway fees. |
| `status` | Success, Failed, etc. |
| `transaction_id` | Remote gateway ID. |
| `refund_amount_usd` | Refunded portion. |
| `is_refunded_flag` | Boolean flag. |
| `fraud_score` | Likelihood of fraud (0-1). |
| `fraud_risk_level` | Categorized (Low, Medium, High). |
| `ip_country` | User IP location. |
| `billing_zip_code` | Payer's zip code. |
| `dq_is_negative_amount` | Quality flag for negative data. |
| `_extracted_at` | Data source grab timestamp. |
| `_loaded_at` | Processing timestamp. |

---

## 4. Delivery (`delivery`)
| Column Name | Business Logic / Description |
|:---|:---|
| `delivery_id` | Primary Key. |
| `order_id` | Foreign Key. |
| `driver_id` | Foreign Key. |
| `restaurant_id` | Foreign Key. |
| `customer_id` | Foreign Key. |
| `pickup_time` | Observed pickup timestamp. |
| `dropoff_time` | Observed delivery timestamp. |
| `duration_min_calculated` | Computed trip duration. |
| `delivery_month` | Month truncation. |
| `delivery_day_of_week` | Day name. |
| `pickup_hour` | Trip start hour. |
| `estimated_time_min` | Predicted duration. |
| `actual_time_min` | Reported duration. |
| `delay_min` | Actual - Estimated. |
| `is_late` | Boolean flag. |
| `distance_km` | Length of trip. |
| `avg_speed_kmh` | Calculated travel speed. |
| `status` | Delivered, Returned, etc. |
| `is_delivered` | Boolean flag. |
| `is_failed` | Boolean flag. |
| `is_in_progress` | Boolean flag. |
| `pickup_lat` / `pickup_lng` | Location points. |
| `dropoff_lat` / `dropoff_lng` | Destination points. |
| `weather_condition` | Stormy, Sunny, etc. |
| `traffic_level` | Severe, Normal, etc. |
| `condition_severity` | Categorized difficulty. |
| `delivery_attempt` | Index of attempt. |
| `is_first_attempt` | Boolean flag. |
| `proof_of_delivery` | Flag for signature/photo. |
| `driver_rating` | Customer rating for driver. |
| `driver_rating_bucket` | Good, Neutral, etc. |
| `tip_amount` | Gratuity. |
| `tip_bucket` | Low, Medium, High. |
| `is_on_time_flag` | Boolean flag. |
| `late_reason` | Explanation for delay. |
| `notes` | Free-text. |
| `dq_on_time_mismatch_...`| Logic consistency flags. |
| `is_redelivery` | Boolean flag. |
| `total_attempts_for_order`| Aggregate attempt count. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

## 5. Drivers (`drivers`)
| Column Name | Business Logic / Description |
|:---|:---|
| `driver_id` | Primary Key. |
| `first_name` | Title-cased. |
| `last_name` | Title-cased. |
| `full_name` | Combined name. |
| `email` | Standardized (lowercase). |
| `phone` | Contact number. |
| `city` / `state` / `zone` | Operational region. |
| `vehicle_type` | Van, Car, Bicycle, etc. |
| `vehicle_plate` | Standardized (uppercase). |
| `license_number` | Standardized (uppercase). |
| `vehicle_capacity` | High, Medium, Low. |
| `hire_date` | Signup date. |
| `months_on_platform` | Tenure duration. |
| `tenure_bucket` | New, Veteran, etc. |
| `is_active` | Boolean flag. |
| `status_raw` | Original status. |
| `status_clean` | Logic-corrected status. |
| `background_check` | Pass, Fail status. |
| `total_deliveries` | Lifetime successful count. |
| `avg_rating` | Performance rating (1-5). |
| `on_time_rate_pct` | Logistics efficiency. |
| `acceptance_rate_pct` | Willingness to work. |
| `rating_bucket` | Excellent, Poor, etc. |
| `performance_tier` | Overall categorization. |
| `hourly_rate_usd` | Comp base. |
| `total_earnings_usd` | Total payouts. |
| `total_tips_usd` | Total gratuities. |
| `avg_earning_per_del_usd`| Computed efficiency. |
| `max_distance_km` | Range capability. |
| `dq_...` | Quality monitoring flags. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

## 6. Restaurants (`restaurants`)
| Column Name | Business Logic / Description |
|:---|:---|
| `restaurant_id` | Primary Key. |
| `restaurant_name` | Title-cased. |
| `cuisine_type` | Title-cased. |
| `address` / `city` / `state` | Location. |
| `zip_code` | Padded 5-digit. |
| `phone` / `email` / `website` | Contact details. |
| `plan_tier` | Basic, Premium, Elite. |
| `monthly_fee_usd` | SaaS recurring revenue. |
| `commission_pct` | Take rate. |
| `commission_bucket` | High, Medium, Low. |
| `signup_date` | Initial onboarding. |
| `months_on_platform` | Lifecycle duration. |
| `is_active` | Boolean flag. |
| `is_featured` | Marketing status. |
| `restaurant_status` | Cleaned activity status. |
| `avg_rating` | Global rating. |
| `total_reviews` | Review volume. |
| `total_orders` | Transaction volume. |
| `avg_prep_time_min` | Kitchen latency. |
| `rating_bucket` | Excellent, Poor, etc. |
| `volume_bucket` | High, Low volume. |
| `est_commissions_earned` | Calculated revenue. |
| `open_time` / `close_time` | Schedule. |
| `operating_hours_per_day` | Calculated availability. |
| `delivery_radius_km` | Logistics range. |
| `min_order_usd` | Minimum basket size. |
| `accepts_cash` | Payment capability. |
| `halal_certified` / `vegan` | Catering features. |
| `dq_...` | Audit flags. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

## 7. Customers (`customers`)
| Column Name | Business Logic / Description |
|:---|:---|
| `customer_id` | Primary Key. |
| `user_id` | Foreign Key. |
| `first_name` / `last_name` | Title-cased. |
| `full_name` | Joint name. |
| `email` | Standardized. |
| `phone_digits` | Digit-only number. |
| `date_of_birth` / `age_years` | Age demography. |
| `gender` | Male, Female, Other. |
| `city` / `state` / `zip_code` | Location (padded zip). |
| `registered_at` | Membership start. |
| `last_order_at` | Last active date. |
| `account_status` | Active, Inactive. |
| `total_orders` | Transaction count. |
| `total_spent_usd` | Lifetime spend. |
| `avg_order_value_usd` | Logic-corrected AOV. |
| `loyalty_points` | Current balance. |
| `loyalty_tier` | Bronze, Silver, etc. |
| `pref_payment_method` | Card, Cash, etc. |
| `pref_cuisine_type` | Pizza, Sushi, etc. |
| `dietary_preference` | Vegan, Keto, etc. |
| `is_premium` | Subscription check. |
| `churn_risk` | Risk score (0-1). |
| `has_marketing_opt_in` | Boolean flag. |
| `referral_count` | Virality. |
| `dq_...` | Consistency flags. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

## 8. Reviews (`reviews`)
| Column Name | Business Logic / Description |
|:---|:---|
| `review_id` | Primary Key. |
| `order_id` | Foreign Key. |
| `customer_id` | Foreign Key. |
| `restaurant_id` | Foreign Key. |
| `driver_id` | Foreign Key. |
| `rating_overall` | Cleaned numeric. |
| `rating_food` / `...` | Specific ratings. |
| `review_content` | Cleaned text content. |
| `review_date` | Event date. |
| `sentiment` | Title-cased (Positive, etc.). |
| `is_verified_purchase` | Boolean check. |
| `helpful_votes_count` | Engagement count. |
| `report_count` | Flagged count. |
| `restaurant_resp_text` | Cleaned reply. |
| `has_restaurant_responded`| Boolean status. |
| `is_featured_review` | Marketing check. |
| `source_platform` | iOS, Android, etc. |
| `has_images` | Attachment flag. |
| `is_positive_review` | Logic flag (rating >= 4). |
| `is_negative_review` | Logic flag (rating <= 2). |
| `is_conflicting_sent...` | Anomaly flag. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

## 9. Billing (`billing`)
| Column Name | Business Logic / Description |
|:---|:---|
| `invoice_id` | Primary Key. |
| `restaurant_id` | Foreign Key. |
| `invoice_date` | Corrected date. |
| `due_date` | Corrected date (handled swap). |
| `payment_date` | Corrected logic-based date. |
| `days_to_due` | Computed duration. |
| `days_to_payment` | Computed duration. |
| `billing_period` | Monthly, Yearly. |
| `plan_tier` | Basic, Premium, etc. |
| `status` | Paid, Pending, etc. |
| `is_overdue` | Current condition flag. |
| `payment_method` | Method used. |
| `payment_gateway` | Gateway used. |
| `currency` | UPPER ISO. |
| `base_fee_usd` | Standard cost. |
| `usage_orders` | Volume metrics. |
| `overage_fee_usd` | Excess cost. |
| `addon_fee_usd` | Extras. |
| `discount_usd` | Deductions. |
| `tax_usd` | Surcharge. |
| `total_usd` | **Logic-recalculated Total**. |
| `total_usd_original` | Source recorded total. |
| `total_usd_variance` | Calculation mismatch. |
| `transaction_ref` | Reference code. |
| `notes` | Free-text. |
| `created_by` | System user. |
| `flag_...` | Individual audit flags. |
| `_loaded_at` | Audit timestamp. |

---

## 10. Promotions (`promotions`)
| Column Name | Business Logic / Description |
|:---|:---|
| `promo_id` | Primary Key. |
| `campaign_id` | Marketing campaign ID. |
| `created_by` | Staff member. |
| `restaurant_id` | NULL if Platform-Wide. |
| `restaurant_scope` | Platform-Wide / Restaurant. |
| `promo_code` | Standardized Uppercase. |
| `promo_name` | Cleaned title. |
| `promo_type` | Percentage, Fixed, etc. |
| `target_segment` | New Users, Dormant, etc. |
| `applies_to` | First Order, etc. |
| `channel` | Email, Push, Social. |
| `is_stackable_flag` | Boolean identifier. |
| `discount_value` | Amount / Percentage. |
| `discount_unit_clean` | Standardized unit. |
| `discount_label` | Formatted label (e.g. $10, 5%). |
| `min_order_usd` | Requirement. |
| `max_discount_usd` | Cap (Logic-corrected). |
| `start_date` / `end_date` | Life interval. |
| `duration_days` | Life length. |
| `is_currently_live` | Logic flag based on DATE. |
| `usage_limit` | Pool size. |
| `times_used` | Redemptions (Capped). |
| `times_used_original` | Raw count. |
| `usage_rate_pct` | Exhaustion progress. |
| `usage_bucket` | Low, Mid, Exhausted. |
| `revenue_impact_usd` | Recorded impact. |
| `revenue_cost_usd` | Absolute cost. |
| `avg_cost_per_use_usd` | Efficiency metric. |
| `dq_...` | Quality audit flags. |
| `_extracted_at` | Audit timestamp. |
| `_loaded_at` | Audit timestamp. |

---

### Deduplication Logic
All tables are automatically deduplicated by keeping only the row with the most recent `_extracted_at` (or `_loaded_at`) for a given primary key.
