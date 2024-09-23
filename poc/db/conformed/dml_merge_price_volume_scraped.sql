MERGE INTO conformed.price_volume_scraped AS target
USING (
    select ticker, date, open, high, low, close, adj_close, volume,
        current_timestamp AT TIME ZONE 'Australia/Brisbane' current_datetime,
        md5(ticker || date || open || high || low || close || adj_close || volume) hash
    from staging.scraped_data
) AS source
ON target.ticker = source.ticker and target.date = source.date
WHEN MATCHED AND target.hash != source.hash THEN
    UPDATE SET
        ticker = source.ticker,
        date = source.date,
        open = source.open,
        high = source.high,
        low = source.low,
        close = source.close,
        adj_close = source.adj_close,
        volume = source.volume,
        updated_datetime = source.current_datetime,
        hash = source.hash
WHEN NOT MATCHED THEN
    INSERT (ticker, date, open, high, low, close, adj_close, volume, created_datetime, updated_datetime, hash)
    VALUES (source.ticker, source.date, source.open, source.high, source.low, source.close, source.adj_close,
            source.volume, source.current_datetime, source.current_datetime, source.hash);