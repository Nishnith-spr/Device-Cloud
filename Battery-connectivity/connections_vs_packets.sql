WITH dates AS (
    SELECT DATE '{{target_date}}' AS max_date
),

base AS (
    SELECT upper(trim(b.battery_oem_no)) AS battery_oem_no,
           substr(upper(b.battery_oem_no), 1, 1) AS oem_prefix,
           MAX_BY(b.country_code, CASE WHEN b.country_code IS NOT NULL THEN b.modified_time ELSE NULL END) AS country_code
    FROM "AWSDataCatalog"."prod_staging_db"."atlas_battery_360_events" b
    CROSS JOIN dates d
    WHERE substr(upper(b.battery_oem_no), 1, 1) IN ('U', 'A', 'C', '7', '8')  -- Removed 'D' (Greenway-2)
      AND length(b.battery_oem_no) IN (15, 18)
      AND CAST(b.modified_time AS DATE) <= d.max_date
      AND b.country_code IS NOT NULL
      AND upper(b.country_code) != 'TZ'               -- Exclude Tanzania
    GROUP BY upper(trim(b.battery_oem_no)), substr(upper(b.battery_oem_no), 1, 1)
),

cbak AS (
    SELECT t.bmsid AS battery_id,
           MAX(from_unixtime(t._time / 1000000000)) AS last_connected,
           MAX_BY(CAST(t.soc AS DOUBLE), t._time) AS soc,
           COUNT(DISTINCT CASE WHEN date_diff('day', CAST(from_unixtime(t._time / 1000000000) AS DATE), d.max_date) < {{health_window}} 
                               THEN date_trunc('hour', from_unixtime(t._time / 1000000000)) END) AS week_pac
    FROM "AWSDataCatalog"."prod_landing_db"."iot_hubtrakmate" t
    CROSS JOIN dates d
    INNER JOIN base b ON upper(t.bmsid) = b.battery_oem_no AND b.oem_prefix = 'C'
    WHERE CAST(t.partition_0 || '-' || t.partition_1 || '-' || t.partition_2 AS DATE) <= d.max_date
      AND lower(t.product) NOT LIKE '%veh%'
    GROUP BY t.bmsid, d.max_date
),

unique_bat AS (
    SELECT t.batid AS battery_id,
           MAX(from_unixtime(t._time / 1000000000)) AS last_connected,
           MAX_BY(CAST(t.soc AS DOUBLE), t._time) AS soc,
           COUNT(DISTINCT CASE WHEN date_diff('day', CAST(from_unixtime(t._time / 1000000000) AS DATE), d.max_date) < {{health_window}} 
                               THEN date_trunc('hour', from_unixtime(t._time / 1000000000)) END) AS week_pac
    FROM "AWSDataCatalog"."prod_landing_db"."iot_hub6" t
    CROSS JOIN dates d
    INNER JOIN base b ON upper(t.batid) = b.battery_oem_no AND b.oem_prefix = 'U'
    WHERE CAST(t.partition_0 || '-' || t.partition_1 || '-' || t.partition_2 AS DATE) <= d.max_date
    GROUP BY t.batid, d.max_date
),

amp_green AS (
    SELECT t.bms_id AS battery_id,
           MAX(FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000)) AS last_connected,
           MAX_BY(CAST(t.battery_soc AS DOUBLE), t.device_time) AS soc,
           COUNT(DISTINCT CASE WHEN date_diff('day', CAST(FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000) AS DATE), d.max_date) < {{health_window}} 
                               THEN date_trunc('hour', FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000)) END) AS week_pac
    FROM "Data-Athena-Prod"."landing_db"."iot_bms_telemetry_critical" t
    CROSS JOIN dates d
    INNER JOIN base b ON upper(t.bms_id) = b.battery_oem_no AND b.oem_prefix IN ('A', '7', '8')
    WHERE CAST(t.year || '-' || t.month || '-' || t.day AS DATE) <= d.max_date
    GROUP BY t.bms_id, d.max_date
),

combined AS (
    SELECT battery_id, last_connected, soc, week_pac FROM cbak
    UNION ALL
    SELECT battery_id, last_connected, soc, week_pac FROM unique_bat
    UNION ALL
    SELECT battery_id, last_connected, soc, week_pac FROM amp_green
),

latest_bms AS (
    SELECT t.bms_id,
           MAX(CAST(t.last_updated_time AS TIMESTAMP)) AS last_connection_attempted
    FROM "Data-Athena-Prod"."landing_db"."latest_bms" t
    CROSS JOIN dates d
    INNER JOIN base b ON upper(t.bms_id) = b.battery_oem_no
    -- Need to check timestamp up to end of max_date
    WHERE CAST(t.last_updated_time AS TIMESTAMP) <= CAST(d.max_date AS TIMESTAMP) + INTERVAL '1' DAY
    GROUP BY t.bms_id
),

temp AS (
    SELECT b.battery_oem_no,
           CASE b.oem_prefix
               WHEN 'C' THEN 'CBAK'
               WHEN 'U' THEN 'Unique'
               WHEN 'A' THEN 'Ampace'
               WHEN '7' THEN 'Greenway-1'
               WHEN '8' THEN 'Greenway-1'
           END AS battery_family,
           c.last_connected,
           date_diff('day', CAST(c.last_connected AS DATE), d.max_date) AS days_from_last_connected,
           l.last_connection_attempted,
           date_diff('day', CAST(l.last_connection_attempted AS DATE), d.max_date) AS days_from_last_connection_attempt,
           date_diff('day', CAST(c.last_connected AS DATE), CAST(l.last_connection_attempted AS DATE)) AS delta_packet_loss_days,
           c.soc,
           c.week_pac,
           b.country_code
    FROM base b
    CROSS JOIN dates d
    LEFT JOIN combined c ON b.battery_oem_no = upper(c.battery_id)
    LEFT JOIN latest_bms l ON b.battery_oem_no = upper(l.bms_id)
),

swap_events AS (
    SELECT oem_in AS oem, modified_time, CAST(current_soc_in AS DOUBLE) AS swapped_soc, 'Swapped in' AS tag
    FROM "prod_landing_db"."atlas_swapping_transactions"
    CROSS JOIN dates d
    WHERE CAST(modified_time AS DATE) >= DATE '2023-01-01'
      AND CAST(modified_time AS DATE) <= d.max_date
    UNION ALL
    SELECT oem_out AS oem, modified_time, CAST(current_soc_out AS DOUBLE) AS swapped_soc, 'Swapped out' AS tag
    FROM "prod_landing_db"."atlas_swapping_transactions"
    CROSS JOIN dates d
    WHERE CAST(modified_time AS DATE) >= DATE '2023-01-01'
      AND CAST(modified_time AS DATE) <= d.max_date
),

swaps AS (
    SELECT upper(e.oem) AS oem,
           MAX(e.modified_time) AS modified_time,
           MAX_BY(e.swapped_soc, e.modified_time) AS swapped_soc,
           MAX_BY(e.tag, e.modified_time) AS tag,
           COUNT(*) AS total_swaps,
           COUNT(CASE WHEN CAST(e.modified_time AS TIMESTAMP) > CAST(t.last_connected AS TIMESTAMP) THEN 1 END) AS swaps_after_last_connected,
           COUNT(CASE WHEN date_diff('day', CAST(e.modified_time AS DATE), d.max_date) <= {{health_window}} THEN 1 END) AS circulation_batteries
    FROM swap_events e
    CROSS JOIN dates d
    INNER JOIN temp t ON upper(e.oem) = t.battery_oem_no
    GROUP BY upper(e.oem), t.last_connected, d.max_date
)

SELECT
    t.*,
    CASE WHEN s.swaps_after_last_connected = 0 THEN s.total_swaps ELSE s.swaps_after_last_connected END AS swaps,
    s.circulation_batteries,
    s.tag,
    s.swapped_soc,
    s.modified_time AS last_swapped,
    COALESCE(s.swapped_soc, t.soc) AS final_soc,
    COALESCE(CAST(s.modified_time AS TIMESTAMP), CAST(t.last_connected AS TIMESTAMP)) AS soc_depletion_ts,
    date_diff('day', CAST(COALESCE(CAST(s.modified_time AS TIMESTAMP), CAST(t.last_connected AS TIMESTAMP)) AS DATE), d.max_date) AS days_for_soc_depletion
FROM temp t
CROSS JOIN dates d
LEFT JOIN swaps s ON t.battery_oem_no = s.oem