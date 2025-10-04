

SELECT * FROM stock_tickers;

CREATE OR REPLACE TABLE stock_tickers(
        ticker VARCHAR,
        name VARCHAR,
        market VARCHAR,
        locale VARCHAR,
        primary_exchange VARCHAR,
        type VARCHAR,
        active BOOLEAN,
        currency_name VARCHAR,
        cik VARCHAR,
        composite_figi VARCHAR,
        share_class_figi VARCHAR,
        last_updated_utc TIMESTAMP,
        date_of_fetch DATE
);