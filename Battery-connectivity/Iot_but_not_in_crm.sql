with iot_hub as(
    SELECT bms_id, last_updated_time AS last_connection_attempted,oem_prefix,country
    FROM (
        SELECT t.bms_id, t.last_updated_time,substr(bms_id, 1, 1) AS oem_prefix,country,
            ROW_NUMBER() OVER (PARTITION BY t.bms_id ORDER BY t.last_updated_time DESC) AS rnk
        FROM "Data-Athena-Prod"."landing_db"."latest_bms" t
        WHERE date(CAST(t.last_updated_time AS TIMESTAMP)) <= DATE '2026-03-08'
        and country in('Kenya','Uganda','Rwanda','Tanzania','Cameroon')
    )
    WHERE rnk = 1
    and oem_prefix in('C','U','7','8','A','D')
    and length(bms_id)in(18,15)
),

crm as (
    SELECT DISTINCT battery_oem_no,substr(battery_oem_no, 1, 1) AS oem_prefix  -- compute once, reuse everywhere
    FROM "AWSDataCatalog"."prod_landing_db"."atlas_battery_360"
    WHERE substr(battery_oem_no, 1, 1) IN ('U', 'A', 'C', '7', '8', 'D')
        AND length(battery_oem_no) IN (15, 18)
        AND date(modified_time) <= DATE '2026-03-08'
)

select country,oem_prefix,count(distinct case when battery_oem_no is null then bms_id else null end  ) as missing_oems 
from(
select a.*,b.battery_oem_no 
from iot_hub a 
left join crm b 
on a.bms_id=b.battery_oem_no
)group by 1,2