-- row count parity
SELECT COUNT(*) AS cnt_wide
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`;

WITH long AS (
  SELECT DISTINCT geohash, dsegId, TIMESTAMP_TRUNC(dateHour, HOUR) AS h
  FROM `stl-datascience.tomtom.tt_bulk_test_geohash6`
)
SELECT COUNT(*) AS cnt_long FROM long;

-- array length check (should always be 4)
SELECT COUNTIF(
  ARRAY_LENGTH(sampleSize_15m)         != 4 OR
  ARRAY_LENGTH(avgSpeed_15m)           != 4 OR
  ARRAY_LENGTH(harmSpeed_15m)          != 4 OR
  ARRAY_LENGTH(medianSpeed_15m)        != 4 OR
  ARRAY_LENGTH(stdSpeed_15m)           != 4 OR
  ARRAY_LENGTH(speedPercentiles_15m)   != 4
) AS bad_rows
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`;

-- total sampleSize parity
SELECT SAFE_DIVIDE(
  (SELECT SUM(sampleSize) FROM `stl-datascience.tomtom.tt_bulk_test_geohash6`),
  (SELECT SUM(total_sampleSize) FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`)
) AS ratio;

-- check for null-padded bins
SELECT COUNT(*) AS hours_with_nulls
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`
WHERE EXISTS (
  SELECT 1
  FROM UNNEST(sampleSize_15m) v
  WHERE v IS NULL
);

-- weighted mean speed parity
WITH src AS (
  SELECT
    geohash, dsegId,
    TIMESTAMP_TRUNC(dateHour, HOUR) AS h,
    SUM(sampleSize * averageSpeedMetersPerHour) AS num,
    SUM(sampleSize) AS den
  FROM `stl-datascience.tomtom.tt_bulk_test_geohash6`
  GROUP BY 1, 2, 3
),
tgt AS (
  SELECT
    geohash, dsegId, dateHour AS h,
    (SELECT SUM(s * a)
     FROM UNNEST(sampleSize_15m) s WITH OFFSET o
     JOIN UNNEST(avgSpeed_15m) a USING (offset)) AS num,
    total_sampleSize AS den
  FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`
)
SELECT COUNT(*) AS mismatches_gt_1pct
FROM src
JOIN tgt USING (geohash, dsegId, h)
WHERE ABS(src.num / src.den - tgt.num / tgt.den) > 0.01;

-- detect duplicates in original 15min bins
SELECT
  geohash, dsegId,
  TIMESTAMP_TRUNC(dateHour, HOUR) AS h,
  DIV(EXTRACT(MINUTE FROM dateHour), 15) AS bin_idx,
  COUNT(*) AS dup_cnt
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6`
GROUP BY 1, 2, 3, 4
HAVING dup_cnt > 1
ORDER BY dup_cnt DESC
LIMIT 20;

-- check monthly partition coverage
SELECT DISTINCT FORMAT_DATE('%Y-%m', DATE_TRUNC(dateHour, MONTH)) AS part
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`
ORDER BY part;

-- inspect null percentile arrays
SELECT
  COUNTIF(p.percentiles IS NULL) AS null_pct_structs,
  COUNT(*) AS total_structs
FROM `stl-datascience.tomtom.tt_bulk_test_geohash6_wide`,
UNNEST(speedPercentiles_15m) AS p;
