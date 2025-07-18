/*─────────────────────────────────────────────────────────────*
 |  long_total_samples.sql                                     |
 |  → total sample size from long table, for given zones       |
 *─────────────────────────────────────────────────────────────*/
WITH
zones AS (
  SELECT * FROM UNNEST(@zone_ids) AS zone_id
),
data_range AS (                       -- keep the yyyymm linkage
  SELECT * FROM UNNEST([@yyyymm]) AS yyyymm
),
inputs AS (
  SELECT zone_id, yyyymm FROM zones, data_range
),
mapped_zones AS (
  SELECT
    i.zone_id,
    l.basemap_yearmonth,
    m.{geohash_column}   AS gh,
    SAFE_CAST(m.tt_dsegid AS INT64) AS dseg_id
  FROM inputs i
  JOIN `stl-datascience.tomtom.dseg_basemap_lookup`     l
    ON i.yyyymm = l.data_yearmonth
  JOIN `stl-datascience.tomtom.dseg_osm_jan25_tt_map_v1` m
    ON  m.yyyymm  = l.basemap_yearmonth
   AND m.zone_id  = i.zone_id
),
src AS (           -- 15‑min rows filtered by zone + time
  SELECT
    z.zone_id,
    a.sampleSize
  FROM `stl-datascience.tomtom.tt_bulk_test_geohash6`  AS a
  JOIN mapped_zones                                    AS z
    ON a.geohash = z.gh
   AND a.dsegId  = z.dseg_id
  WHERE a.dateHour >= @from_ts
    AND a.dateHour <  @to_ts
)
SELECT
  zone_id,
  SUM(sampleSize) AS total_samples
FROM src
GROUP BY zone_id;


--------


/*─────────────────────────────────────────────────────────────*
 |  wide_total_samples.sql                                    |
 |  → same result using the new wide table                    |
 *─────────────────────────────────────────────────────────────*/
WITH
zones AS (
  SELECT * FROM UNNEST(@zone_ids) AS zone_id
),
data_range AS (
  SELECT * FROM UNNEST([@yyyymm]) AS yyyymm
),
inputs AS (
  SELECT zone_id, yyyymm FROM zones, data_range
),
mapped_zones AS (
  SELECT
    i.zone_id,
    l.basemap_yearmonth,
    m.{geohash_column}   AS gh,
    SAFE_CAST(m.tt_dsegid AS INT64) AS dseg_id
  FROM inputs i
  JOIN `stl-datascience.tomtom.dseg_basemap_lookup`     l
    ON i.yyyymm = l.data_yearmonth
  JOIN `stl-datascience.tomtom.dseg_osm_jan25_tt_map_v1` m
    ON  m.yyyymm  = l.basemap_yearmonth
   AND m.zone_id  = i.zone_id
),
src AS (           -- explode 4×15‑min bins per hour
  SELECT
    z.zone_id,
    s.val AS sampleSize,
    TIMESTAMP_ADD(a.dateHour, INTERVAL OFFSET(s)*15 MINUTE) AS slot_ts
  FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide` AS a
  JOIN mapped_zones                                        AS z
    ON a.geohash = z.gh
   AND a.dsegId  = z.dseg_id
  CROSS JOIN UNNEST(a.sampleSize_15m) AS s               -- s is STRUCT<val INT64>
  WHERE slot_ts >= @from_ts
    AND slot_ts <  @to_ts
)
SELECT
  zone_id,
  SUM(sampleSize) AS total_samples
FROM src
GROUP BY zone_id;
