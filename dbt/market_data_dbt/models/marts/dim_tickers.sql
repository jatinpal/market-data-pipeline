SELECT DISTINCT
    ticker,
    -- Add metadata columns as needed later
    CURRENT_TIMESTAMP() as created_at
FROM {{ ref('stg_daily_prices') }}