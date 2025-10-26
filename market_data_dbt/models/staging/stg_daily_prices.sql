SELECT
    CAST(Date AS DATE) as date,
    'AAPL' as ticker,
    CAST(Open AS FLOAT64) as open_price,
    CAST(High AS FLOAT64) as high_price,
    CAST(Low AS FLOAT64) as low_price,
    CAST(Close AS FLOAT64) as close_price,
    CAST(Volume AS INT64) as volume,
    CAST(Dividends AS FLOAT64) as dividends,
    CAST(`Stock Splits` AS FLOAT64) as stock_splits
FROM {{ source('raw_data', 'aapl_raw') }}