-- LINE FORECAST
-- Where each line's delay is heading over the next 10 minutes.
--
-- ML_FORECAST is the built-in TimesFM forecaster -- no CREATE MODEL, no
-- connection object. It returns an ARRAY<ROW> directly, so the access is
-- forecast_result[10].forecast_value, not forecast_result.forecast[10].mean.

CREATE TABLE `line_forecast` AS
WITH enriched AS (
  SELECT route_id, direction, delay_sec, `$rowtime` AS event_time
  FROM `trip_updates_clean`
  WHERE direction IS NOT NULL
),
windowed AS (
  SELECT
    route_id,
    direction,
    window_time,
    AVG(CAST(delay_sec AS DOUBLE)) AS avg_delay
  FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '60' SECOND))
  GROUP BY route_id, direction, window_start, window_end, window_time
),
forecast AS (
  SELECT
    *,
    ML_FORECAST(
      avg_delay,
      window_time,
      JSON_OBJECT('horizon' VALUE 10, 'minContextSize' VALUE 15, 'maxContextSize' VALUE 60, 'rmseWindowSize' VALUE 5)
    ) OVER (
      PARTITION BY route_id, direction
      ORDER BY window_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS forecast_result
  FROM windowed
)
SELECT
  route_id,
  direction,
  window_time,
  avg_delay,
  forecast_result[5].forecast_value  AS delay_in_5m,
  forecast_result[10].forecast_value AS delay_in_10m,
  (forecast_result[10].forecast_value > 2 * avg_delay
   AND forecast_result[10].forecast_value > 240) AS predicted_bunching
FROM forecast;
