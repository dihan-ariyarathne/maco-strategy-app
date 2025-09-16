CREATE OR REPLACE VIEW `finnhubmacostrategy.analytics.v_candles_with_signals` AS
SELECT
  p.trade_date,
  p.symbol,
  p.open,
  p.high,
  p.low,
  p.close,
  f.sma_fast,
  f.sma_slow,
  s.signal,
  s.crossover,
  n.predicted_signal,
  n.predicted_direction,
  n.predicted_close,
  n.generated_at
FROM `finnhubmacostrategy.prices_1d.prices_1d` p
LEFT JOIN `finnhubmacostrategy.maco_features.maco_features` f
  ON f.symbol = p.symbol AND f.trade_date = p.trade_date
LEFT JOIN `finnhubmacostrategy.signals_maco.signals_maco` s
  ON s.symbol = p.symbol AND s.trade_date = p.trade_date
LEFT JOIN `finnhubmacostrategy.pred_next_day.pred_next_day` n
  ON n.symbol = p.symbol AND n.trade_date = p.trade_date
WHERE p.trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR);