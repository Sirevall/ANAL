CREATE_MAIN_TABLE =
CREATE TABLE IF NOT EXISTS sirevall.crypto_rates_raw_h1_v2 (
    asset VARCHAR(100),
    datetime TIMESTAMP,
    rate FLOAT(8)
);

TRUNCATE_TABLE =
TRUNCATE TABLE sirevall.crypto_rates_raw_h1_v2;

INSERT_DATA =
INSERT INTO sirevall.crypto_rates_raw_h1_v2 (asset, datetime, rate) VALUES %s;

CREATE_AGG_TABLE =
CREATE TABLE IF NOT EXISTS sirevall.crypto_rates_agg_v2 AS
SELECT
    asset,
    datetime AS hour,
    AVG(rate) AS avg_price,
    MAX(rate) AS max_price,
    MIN(rate) AS min_price,
    AVG(rate) OVER (PARTITION BY asset, DATE(datetime)) AS daily_avg_price
FROM
    sirevall.crypto_rates_raw_h1_v2
GROUP BY
    asset, datetime, rate
ORDER BY
    asset, hour;