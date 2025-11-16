SELECT DISTINCT
    ticker,
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('stg_daily_prices') }}
ORDER BY ticker