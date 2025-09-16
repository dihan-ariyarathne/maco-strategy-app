CREATE OR REPLACE VIEW `finnhubmacostrategy.analytics.v_latest_next_day_cards` AS
WITH ranked AS (
  SELECT
    symbol,
    trade_date,
    predicted_signal,
    predicted_direction,
    predicted_close,
    generated_at,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC, generated_at DESC) AS rn
  FROM `finnhubmacostrategy.pred_next_day.pred_next_day`
  WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;