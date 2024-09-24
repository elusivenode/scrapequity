CREATE OR REPLACE PROCEDURE conformed.sp_merge_into_price_volume_scraped()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Your procedure logic here
    MERGE INTO conformed.price_volume_scraped AS target
    USING (
        select p.ticker, p.date, p.open, p.high, p.low, p.close, p.adj_close, p.volume,
            coalesce(d.dividend,0) dividend,
            current_timestamp AT TIME ZONE 'Australia/Brisbane' current_datetime,
            md5(p.ticker || p.date || p.open || p.high || p.low || p.close || p.adj_close || p.volume || d.dividend) hash
        from staging.scraped_data_price_volume p
            left join staging.scraped_data_dividend d
                on p.ticker = d.ticker and p.date = d.date
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
            dividend = source.dividend,
            updated_datetime = source.current_datetime,
            hash = source.hash
    WHEN NOT MATCHED THEN
        INSERT (ticker, date, open, high, low, close, adj_close, volume, dividend, created_datetime, updated_datetime, hash)
        VALUES (source.ticker, source.date, source.open, source.high, source.low, source.close, source.adj_close,
                source.volume, source.dividend, source.current_datetime, source.current_datetime, source.hash);
END;
$$;