WITH active_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email_address,
        created_at,
        updated_at
    FROM raw_db.raw.raw_users
    WHERE active = true
),
user_stats AS (
    SELECT
        user_id,
        count(*) as order_count,
        sum(amount) as total_spent
    FROM raw_db.raw.raw_orders
    GROUP BY user_id
)
SELECT
    au.user_id as id,
    au.first_name as name,
    au.email_address as email
FROM active_users au
LEFT JOIN user_stats us ON au.user_id = us.user_id
