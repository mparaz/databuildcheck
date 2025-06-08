SELECT
    id,
    user_id,
    amount
FROM dev_db.staging.raw_orders
WHERE status = 'completed'
