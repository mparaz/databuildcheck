WITH user_details AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email_address,
        phone_number,  -- This column is NOT in the final SELECT
        address,       -- This column is NOT in the final SELECT
        created_at,
        updated_at
    FROM raw_users
    WHERE active = true
)
SELECT
    user_id as id,
    first_name as name,
    email_address as email
FROM user_details
