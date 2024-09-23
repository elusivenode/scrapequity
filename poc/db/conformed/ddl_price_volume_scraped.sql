drop table if exists conformed.price_volume_scraped;
create table conformed.price_volume_scraped
(
    id        serial,
    ticker    varchar(10),
    date      date,
    open      double precision,
    high      double precision,
    low       double precision,
    close     double precision,
    adj_close double precision,
    volume    bigint,
    created_datetime timestamptz,
    updated_datetime timestamptz,
    hash char(32),
    primary key (ticker, date)
);

alter table staging.scraped_data
    owner to postgres;