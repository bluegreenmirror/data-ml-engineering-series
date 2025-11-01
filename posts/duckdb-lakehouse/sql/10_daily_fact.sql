
WITH base AS (
  SELECT user_id, event_ts, DATE_TRUNC('day', event_ts) AS day, country,
         (revenue - cost) AS profit
  FROM read_parquet('../data/events/date=*/part-*.parquet')
  WHERE day BETWEEN DATE ':start_date' AND DATE ':end_date'
    AND country = 'US'
),
enriched AS (
  SELECT b.user_id, b.country, b.day, DATE_TRUNC('hour', b.event_ts) AS hour, b.profit, u.tier, u.region
  FROM base b LEFT JOIN read_parquet('../data/dim/users.parquet') u USING (user_id)
)
SELECT hour, tier, COUNT(*) AS events, SUM(profit) AS profit_sum, AVG(profit) AS profit_avg
FROM enriched
GROUP BY hour, tier
ORDER BY hour, tier;
