WITH dates AS (
    SELECT date('2026-03-08') AS max_date
),

base AS (
    SELECT *
    FROM (
        SELECT DISTINCT upper(battery_oem_no) as battery_oem_no,
        substr(battery_oem_no, 1, 1) AS oem_prefix,
        ROW_NUMBER() OVER (PARTITION BY battery_oem_no ORDER BY modified_time DESC) AS rnk
        FROM "AWSDataCatalog"."prod_landing_db"."atlas_battery_360"
        CROSS JOIN dates
        WHERE substr(battery_oem_no, 1, 1) IN ('U', 'A', 'C', '7', '8', 'D')
        AND length(battery_oem_no) IN (15, 18)
        AND date(modified_time) <= max_date
    )WHERE rnk = 1
),

cbak AS (
    SELECT bmsid,event_time,soc,
    ROW_NUMBER() OVER (PARTITION BY bmsid ORDER BY _time DESC) AS rnk,
    SUM(is_new_hour) OVER (PARTITION BY bmsid ORDER BY _time)   AS week_pac
    FROM (
        SELECT
        t.bmsid,t._time,from_unixtime(t._time / 1000000000)AS event_time,CAST(t.soc AS DOUBLE)AS soc,
        CASE WHEN date_diff('day',date(from_unixtime(t._time / 1000000000)), d.max_date)>=7  THEN 0
        WHEN LAG(hour(from_unixtime(t._time / 1000000000)))OVER (PARTITION BY t.bmsid ORDER BY t._time)=hour(from_unixtime(t._time / 1000000000))THEN 0 ELSE 1 END AS is_new_hour
        FROM "AWSDataCatalog"."prod_landing_db"."iot_hubtrakmate" t
        CROSS JOIN dates d
        INNER JOIN base b ON t.bmsid = b.battery_oem_no AND b.oem_prefix = 'C'
        WHERE date(t.partition_0 || '-' || t.partition_1 || '-' || t.partition_2) <= d.max_date
        and lower(product) not like '%veh%' 
    )
),

unique_bat AS (
    SELECT batid,event_time,soc,
    ROW_NUMBER() OVER (PARTITION BY batid ORDER BY _time DESC) AS rnk,
    SUM(is_new_hour) OVER (PARTITION BY batid ORDER BY _time)   AS week_pac
    FROM (
        SELECT t.batid,t._time,from_unixtime(t._time / 1000000000) AS event_time,CAST(t.soc AS DOUBLE) AS soc,
        CASE WHEN date_diff('day',date(from_unixtime(t._time / 1000000000)),d.max_date)>=7  THEN 0
        WHEN LAG(hour(from_unixtime(t._time / 1000000000)))OVER (PARTITION BY t.batid ORDER BY t._time)=hour(from_unixtime(t._time / 1000000000))THEN 0 ELSE 1 END AS is_new_hour
        FROM "AWSDataCatalog"."prod_landing_db"."iot_hub6" t
        CROSS JOIN dates d
        INNER JOIN base b ON t.batid = b.battery_oem_no AND b.oem_prefix = 'U'
        WHERE date(t.partition_0 || '-' || t.partition_1 || '-' || t.partition_2) <= d.max_date
    )
),

amp_green AS (
    SELECT bms_id,event_time,soc,
    ROW_NUMBER() OVER (PARTITION BY bms_id ORDER BY device_time DESC) AS rnk,
    SUM(is_new_hour) OVER (PARTITION BY bms_id ORDER BY device_time)   AS week_pac
    FROM (
        SELECT t.bms_id,t.device_time,FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000)  AS event_time,CAST(t.battery_soc AS DOUBLE) AS soc,
        CASE
        WHEN date_diff('day',date(FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000)), d.max_date)>=7  THEN 0
        WHEN LAG(hour(FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000)))OVER (PARTITION BY t.bms_id ORDER BY t.device_time)= hour(FROM_UNIXTIME(CAST(t.device_time AS DOUBLE) / 1000))THEN 0 ELSE 1 END AS is_new_hour
        FROM "Data-Athena-Prod"."landing_db"."iot_bms_telemetry_critical" t
        CROSS JOIN dates d
        INNER JOIN base b ON t.bms_id = b.battery_oem_no AND b.oem_prefix IN ('A', '7', '8')
        WHERE date(t.year || '-' || t.month || '-' || t.day) <= d.max_date
    )
),

combined AS (
    SELECT bmsid AS battery_id, event_time AS last_connected, soc, week_pac FROM cbak WHERE rnk = 1
    UNION ALL
    SELECT batid,event_time,soc,week_pac FROM unique_bat WHERE rnk = 1
    UNION ALL
    SELECT bms_id,event_time,soc,week_pac FROM amp_green  WHERE rnk = 1
),

latest_bms AS (
    SELECT bms_id, last_updated_time AS last_connection_attempted
    FROM (
        SELECT t.bms_id,t.last_updated_time,
        ROW_NUMBER() OVER (PARTITION BY t.bms_id ORDER BY t.last_updated_time DESC) AS rnk
        FROM "Data-Athena-Prod"."landing_db"."latest_bms" t
        CROSS JOIN dates d
        INNER JOIN base b ON t.bms_id = b.battery_oem_no
        WHERE date(CAST(t.last_updated_time AS TIMESTAMP)) <= d.max_date
    )WHERE rnk = 1
),

temp AS (
    select a.*, b.country_code 
    from(
    SELECT b.battery_oem_no,
    CASE b.oem_prefix
    WHEN 'C' THEN 'CBAK'
    WHEN 'U' THEN 'Unique'
    WHEN 'A' THEN 'Ampace'
    WHEN '7' THEN 'Greenway-1'
    WHEN '8' THEN 'Greenway-1'
    WHEN 'D' THEN 'Greenway-2'
    END AS battery_family,c.last_connected,
        -- FIX: was hardcoded DATE '2026-03-01'; now uses max_date consistently
    date_diff('day', c.last_connected, CAST(d.max_date AS TIMESTAMP)) AS days_from_last_connected,
    CAST(l.last_connection_attempted AS TIMESTAMP) AS last_connection_attempted,
    date_diff('day', CAST(l.last_connection_attempted AS TIMESTAMP),
    CAST(d.max_date AS TIMESTAMP))AS days_from_last_connection_attempt,
    date_diff('day', c.last_connected,CAST(l.last_connection_attempted AS TIMESTAMP))AS delta_packet_loss_days,
    c.soc,
    c.week_pac
    FROM base b
    CROSS JOIN dates d
    LEFT JOIN combined   c ON b.battery_oem_no = c.battery_id
    LEFT JOIN latest_bms l ON b.battery_oem_no = l.bms_id
    )a left join(
    SELECT *
    FROM (
        SELECT DISTINCT upper(battery_oem_no) as battery_oem_no,country_code,
        ROW_NUMBER() OVER (PARTITION BY battery_oem_no ORDER BY modified_time DESC) AS rnk
        FROM "AWSDataCatalog"."prod_landing_db"."atlas_battery_360"
        CROSS JOIN dates
        WHERE substr(battery_oem_no, 1, 1) IN ('U', 'A', 'C', '7', '8', 'D')
        AND length(battery_oem_no) IN (15, 18)
        AND date(modified_time) <= max_date
        and country_code is not null
    )WHERE rnk = 1
    )b on a.battery_oem_no=b.battery_oem_no
),

swap_events AS (
    -- Swapped IN
    SELECT DISTINCT a.oem_in AS oem,a.modified_time,CAST(a.current_soc_in AS DOUBLE) AS swapped_soc,'Swapped in'AS tag,
    count(case when cast(modified_time as timestamp)>cast(last_connected as timestamp) then oem_in else null end)over(partition by oem_in order by modified_time) as swaps
    FROM "prod_landing_db"."atlas_swapping_transactions" a
    CROSS JOIN dates d
    INNER JOIN temp b ON a.oem_in = b.battery_oem_no
    AND date(a.modified_time) >=  TIMESTAMP '2023-01-01 00:00:00'
    WHERE date(a.modified_time) <= d.max_date

    UNION ALL

    -- Swapped OUT
    SELECT DISTINCT a.oem_out AS oem,a.modified_time,CAST(a.current_soc_out AS DOUBLE) AS swapped_soc,'Swapped out'AS tag,
    count(case when cast(modified_time as timestamp)>cast(last_connected as timestamp) then oem_out else null end)over(partition by oem_out order by modified_time) as swaps
    FROM "prod_landing_db"."atlas_swapping_transactions" a
    CROSS JOIN dates d
    INNER JOIN temp b ON a.oem_out = b.battery_oem_no
    AND a.modified_time >=  TIMESTAMP '2023-01-01 00:00:00'
    WHERE date(a.modified_time) <= d.max_date
),

swaps AS (
    SELECT oem,modified_time,swapped_soc,tag,
    case when sum(swaps)over(PARTITION BY oem ORDER BY modified_time)=0 then 
    count(*)over(partition by oem order by modified_time)
    else sum(swaps)over(PARTITION BY oem ORDER BY modified_time) end 
    as swaps,
        -- FIX: was missing END keyword and had broken subquery syntax
        COUNT(CASE WHEN date_diff('day',date(modified_time), d.max_date)<=7  THEN oem ELSE NULL END)
        OVER (PARTITION BY oem ORDER BY modified_time) AS circulation_batteries,
        ROW_NUMBER() OVER (PARTITION BY oem ORDER BY modified_time DESC) AS rnk
    FROM swap_events
    CROSS JOIN dates d
)

SELECT
    t.*,
    s.swaps,
    s.circulation_batteries,
    s.tag,
    s.swapped_soc,
    s.modified_time AS last_swapped,
    COALESCE(s.swapped_soc, t.soc) AS final_soc,
    COALESCE(s.modified_time, t.last_connected) AS soc_depletion_ts,
    date_diff('day',COALESCE(s.modified_time, t.last_connected),CAST(d.max_date AS TIMESTAMP))AS days_for_soc_depletion
FROM temp t
CROSS JOIN dates d
LEFT JOIN swaps s ON t.battery_oem_no = s.oem AND s.rnk = 1