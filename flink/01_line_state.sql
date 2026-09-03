-- LINE STATE
-- Per route + direction, one row every 60s: how late the line is, how bunched it
-- is, and whether the average delay is anomalous against that line's own history.
--
-- ML_DETECT_ANOMALIES is a built-in Confluent AI function -- no CREATE MODEL and
-- no connection object required. minTrainingSize=10 means roughly 10 minutes of
-- per-line history before anomalies begin firing.

CREATE TABLE `line_state`
WITH ('changelog.mode' = 'append') AS
WITH enriched AS (
  -- $rowtime is a system column on physical tables and does not survive into a
  -- CTE, so it is aliased here and the TUMBLE descriptor points at the alias.
  SELECT route_id, direction, trip_id, stop_name, lat, lon, delay_sec, synthetic,
         `$rowtime` AS event_time
  FROM `trip_updates_clean`
  WHERE direction IS NOT NULL
),
windowed AS (
  SELECT
    route_id,
    direction,
    window_start,
    window_end,
    window_time,
    AVG(CAST(delay_sec AS DOUBLE))                             AS avg_delay,
    MAX(delay_sec)                                             AS max_delay,
    COUNT(DISTINCT trip_id)                                    AS active_trains,
    -- DISTINCT trip_id, not COUNT(*): the feed emits one row per upcoming stop,
    -- so counting rows would count a single late train dozens of times.
    COUNT(DISTINCT CASE WHEN delay_sec > 300 THEN trip_id END) AS late_trains,
    MAX(synthetic)                                             AS synthetic,
    -- single-pass argmax: the widest delay sorts last, so MAX() returns that
    -- row's location. Keeps this a plain window aggregate (append-only, no join).
    MAX(CONCAT(
      LPAD(CAST(GREATEST(delay_sec, 0) AS STRING), 6, '0'), '~',
      COALESCE(stop_name, ''), '~',
      CAST(lat AS STRING), '~',
      CAST(lon AS STRING)
    ))                                                         AS worst_packed
  FROM TABLE(TUMBLE(TABLE enriched, DESCRIPTOR(event_time), INTERVAL '60' SECOND))
  GROUP BY route_id, direction, window_start, window_end, window_time
),
anomaly AS (
  SELECT
    *,
    ML_DETECT_ANOMALIES(
      avg_delay,
      window_time,
      JSON_OBJECT('minTrainingSize' VALUE 10, 'maxTrainingSize' VALUE 60, 'confidencePercentage' VALUE 99.0, 'enableStl' VALUE FALSE)
    ) OVER (
      PARTITION BY route_id, direction
      ORDER BY window_time
      RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS anomaly_result
  FROM windowed
),
flagged AS (
  -- separate CTE only because Flink cannot reference a SELECT alias in the same list
  SELECT
    *,
    (anomaly_result.is_anomaly AND anomaly_result.actual_value > anomaly_result.upper_bound)
      AS anomaly_delay
  FROM anomaly
)
SELECT
  route_id,
  direction,
  window_start,
  window_end,
  window_time,
  avg_delay,
  max_delay,
  active_trains,
  late_trains,
  anomaly_delay,
  SPLIT_INDEX(worst_packed, '~', 1)                 AS worst_stop_name,
  CAST(SPLIT_INDEX(worst_packed, '~', 2) AS DOUBLE) AS worst_lat,
  CAST(SPLIT_INDEX(worst_packed, '~', 3) AS DOUBLE) AS worst_lon,
  CASE
    WHEN anomaly_delay AND late_trains >= 3 THEN 'critical'
    WHEN anomaly_delay OR  late_trains >= 2 THEN 'warn'
    ELSE 'ok'
  END                                               AS severity,
  late_trains * 800                                 AS riders_low,
  late_trains * 1800                                AS riders_high,
  CASE
    WHEN late_trains <= 2 THEN 'low'
    WHEN late_trains <= 4 THEN 'medium'
    ELSE 'high'
  END                                               AS confidence,
  synthetic
FROM flagged;
