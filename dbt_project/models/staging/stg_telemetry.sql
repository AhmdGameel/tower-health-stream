-- Reads validated streaming telemetry materialized from the Iceberg REST Catalog
-- (telecom.valid_events) into DuckDB by services/analytics_runner.py
select
    cast(tower_id as bigint)        as tower_id,
    trim(operator)                  as operator,
    upper(trim(radio))              as radio,
    cast(area as bigint)            as area,
    cast(ts as timestamp)           as event_ts,
    event_date,
    cast(signal_strength as double) as signal_strength,
    cast(latency_ms as double)      as latency_ms,
    cast(connected_users as bigint) as connected_users,
    cast(call_drop_rate as double)  as call_drop_rate,
    cast(is_peak_hour as boolean)   as is_peak_hour,
    cast(is_anomaly as boolean)     as is_anomaly
from main.raw_iceberg_telemetry
where tower_id is not null
