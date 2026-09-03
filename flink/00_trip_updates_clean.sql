-- CLEANING LAYER
-- Deduplicates the raw feed, drops unusable rows, and normalizes the fields that
-- every downstream statement depends on. Nothing below reads mta.trip_updates directly.
--
-- Window deduplication (not plain ROW_NUMBER) is what keeps this append-only:
-- keep-last-row dedup emits retractions, which an append sink cannot consume.
-- Tumbling the dedup means the window closes and emits exactly once.

CREATE TABLE `trip_updates_clean`
WITH ('changelog.mode' = 'append') AS
SELECT
  trip_id,
  stop_id,
  route_id,
  stop_name,
  lat,
  lon,
  direction,
  arrival_time,
  feed_time,
  delay_sec,
  vehicle_status,
  synthetic
FROM (
  SELECT
    trip_id,
    stop_id,
    UPPER(route_id)                                     AS route_id,
    stop_name,
    lat,
    lon,
    -- fall back to the NYCT stop_id suffix when the feed omits direction
    COALESCE(
      NULLIF(direction, ''),
      CASE WHEN SUBSTRING(stop_id, CHAR_LENGTH(stop_id), 1) IN ('N', 'S')
           THEN SUBSTRING(stop_id, CHAR_LENGTH(stop_id), 1)
      END
    )                                                   AS direction,
    TO_TIMESTAMP_LTZ(arrival_ts, 0)                     AS arrival_time,
    TO_TIMESTAMP_LTZ(feed_ts, 0)                        AS feed_time,
    delay_sec,
    COALESCE(vehicle_status, 'UNKNOWN')                 AS vehicle_status,
    synthetic,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end, trip_id, stop_id
      ORDER BY `$rowtime` DESC
    ) AS row_num
  FROM TABLE(TUMBLE(TABLE `mta.trip_updates`, DESCRIPTOR(`$rowtime`), INTERVAL '60' SECOND))
  WHERE lat IS NOT NULL
    AND lon IS NOT NULL
    AND delay_sec >= -600
    AND delay_sec <= 7200
)
WHERE row_num = 1;
