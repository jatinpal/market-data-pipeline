SELECT
    CONCAT(ticker, '_', CAST(date AS STRING)) as market_daily_key,
    date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    dividends,
    stock_splits
FROM {{ ref('stg_daily_prices') }}