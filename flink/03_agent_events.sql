-- AGENT EVENTS
-- Rider-facing alerts consumed by the map as countdown-clock cards.
-- Deterministic by design: pure string and arithmetic expressions.
-- No LLM, no model, no connection object.
--
-- Two sources, unioned into one topic:
--   1. line_state       -- a line is disrupted right now
--   2. line_forecast    -- a line is predicted to bunch within ~10 minutes
-- The forecast branch joins back to line_state purely to pick up coordinates,
-- since line_forecast carries no lat/lon and the map drops cards without them.

CREATE TABLE `headway.agent_events` (
  id STRING,
  route_id STRING,
  severity STRING,
  title STRING,
  body STRING,
  lat DOUBLE,
  lon DOUBLE,
  riders_affected INT,
  ts TIMESTAMP_LTZ(3)
) DISTRIBUTED INTO 6 BUCKETS
WITH ('changelog.mode' = 'append', 'value.format' = 'avro-registry');


INSERT INTO `headway.agent_events`
SELECT
  CONCAT('ls-', route_id, '-', direction, '-', CAST(window_time AS STRING))     AS id,
  route_id,
  severity,
  CONCAT(route_id, ' ', worst_stop_name, ': trains bunching')                   AS title,
  CONCAT(
    'Severity ', severity, '. ',
    CAST(late_trains AS STRING), ' late trains on ', route_id, ' ', direction, '. ',
    'Estimated ', CAST(riders_low AS STRING), ' to ', CAST(riders_high AS STRING),
    ' riders affected. Confidence ', confidence, '.'
  )                                                                             AS body,
  worst_lat                                                                     AS lat,
  worst_lon                                                                     AS lon,
  CAST((riders_low + riders_high) / 2 AS INT)                                   AS riders_affected,
  window_time                                                                   AS ts
FROM `line_state`
WHERE severity <> 'ok'
UNION ALL
SELECT
  CONCAT('fc-', f.route_id, '-', f.direction, '-', CAST(f.window_time AS STRING)) AS id,
  f.route_id,
  'warn'                                                                        AS severity,
  CONCAT(f.route_id, ' bunching predicted in ~10 min')                          AS title,
  CONCAT(
    'Forecast: average delay on ', f.route_id, ' ', f.direction,
    ' rises from ', CAST(CAST(f.avg_delay AS INT) AS STRING), 's to ',
    CAST(CAST(f.delay_in_10m AS INT) AS STRING), 's within 10 minutes.'
  )                                                                             AS body,
  l.worst_lat                                                                   AS lat,
  l.worst_lon                                                                   AS lon,
  CAST((l.riders_low + l.riders_high) / 2 AS INT)                               AS riders_affected,
  f.window_time                                                                 AS ts
FROM `line_forecast` f
JOIN `line_state` l
  ON f.route_id = l.route_id
 AND f.direction = l.direction
 AND l.`$rowtime` BETWEEN f.`$rowtime` - INTERVAL '2' MINUTE AND f.`$rowtime`
WHERE f.predicted_bunching;
