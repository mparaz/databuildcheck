WITH active_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email_address,
        'active' as user_type
    FROM raw_db.raw.raw_users
    WHERE active = true
),
inactive_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email_address,
        'inactive' as user_type
    FROM raw_db.raw.raw_users
    WHERE active = false
)
SELECT
    user_id as id,
    first_name as name,
    email_address as email,
    user_type
FROM active_users
UNION ALL
SELECT
    user_id as id,
    first_name as name,
    email_address as email,
    user_type
FROM inactive_users
ORDER BY user_type, id
