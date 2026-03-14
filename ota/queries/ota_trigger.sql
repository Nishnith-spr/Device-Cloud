
	select * from(
		select *
		from(
			select a.*,cast(b.started_at as timestamp)+interval'330'minute as task_started_at,
			cast(b.requested_at as timestamp)+interval'330'minute as task_requested_at
			from (
				select a.*, b.name as device_name,
				b.status as device_status,cast(b.created_at as timestamp)+interval'330'minute as device_creation_Date,
				cast(b.connected_at as timestamp)+interval'330'minute as device_last_connected,
				cast(b.disconnected_at as timestamp)+interval'330'minute as device_disconnected,
				b.imei as device_imei,b.bmsid,b.batid,b.iccid as device_iccid,b.soc,b.latitude as device_latitude,
				b.longitude as device_longitude,b.bmsversion,iotversion,b.supplier as device_supplier,
				b.model_nme as device_model
				from(
					select a.*,b.version_number as target_version, 
					b.release_notes as target_notes,b.is_active as firmware_active,
					b.name as firmware_device_name
					from(
						select id, 
						status,
						cast(initiated_at as timestamp)+interval'330'minute as initiated_at,
						cast(completed_at as timestamp)+interval'330'minute as completed_at,
						message,
						device_id,
						firmware_version_id,
						task_id ,
						cast(requested_at as timestamp)+interval'330'minute as requested_at,
						base_fw_version
						from ota_otaupdate 
					) a  
					left join 
					ota_firmwareversion b
					on a.firmware_version_id=b.id
				)a left join 
				ota_device b 
				on a.device_id=b.id
			)a left join ota_task b 
			on a.task_id=b.id
		)a
	)a