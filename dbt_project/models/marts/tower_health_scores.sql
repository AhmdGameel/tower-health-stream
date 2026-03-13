with towers as (
    select * from {{ ref('stg_towers') }}
),

telemetry_agg as (
    select
        tower_id,
        count(*)                                                    as event_count,
        round(avg(signal_strength), 2)                              as avg_signal_dbm,
        round(avg(latency_ms), 2)                                   as avg_latency_ms,
        round(avg(call_drop_rate) * 100.0, 2)                       as avg_drop_rate_pct,
        round(avg(connected_users), 1)                              as avg_connected_users,
        round(sum(case when is_anomaly then 1 else 0 end) * 100.0 / count(*), 2) as anomaly_rate_pct
    from {{ ref('stg_telemetry') }}
    group by tower_id
),

scored as (
    select
        t.tower_id,
        t.operator,
        t.radio,
        t.lat,
        t.lon,
        t.area,
        coalesce(a.event_count, 0)         as event_count,
        a.avg_signal_dbm,
        a.avg_latency_ms,
        a.avg_drop_rate_pct,
        a.avg_connected_users,
        coalesce(a.anomaly_rate_pct, 0.0)  as anomaly_rate_pct,

        -- Radio baseline score (0-100)
        case
            when t.radio = 'LTE'  then 100.0
            when t.radio = 'UMTS' then 75.0
            when t.radio = 'GSM'  then 50.0
            else 40.0
        end as radio_score,

        -- Signal quality score (0-100): -60 dBm -> 100, -110 dBm -> 0
        case
            when a.avg_signal_dbm is null then null
            else greatest(0.0, least(100.0, (a.avg_signal_dbm + 110.0) * 2.0))
        end as signal_score,

        -- Drop rate score (0-100): 0% -> 100, >=20% -> 0
        case
            when a.avg_drop_rate_pct is null then null
            else greatest(0.0, least(100.0, 100.0 - (a.avg_drop_rate_pct * 5.0)))
        end as drop_score,

        -- Latency score (0-100): 20ms -> 100, >=220ms -> 0
        case
            when a.avg_latency_ms is null then null
            else greatest(0.0, least(100.0, 100.0 - ((a.avg_latency_ms - 20.0) * 0.5)))
        end as latency_score
    from towers t
    left join telemetry_agg a on t.tower_id = a.tower_id
)

select
    tower_id,
    operator,
    radio,
    lat,
    lon,
    area,
    event_count,
    avg_signal_dbm,
    avg_latency_ms,
    avg_drop_rate_pct,
    avg_connected_users,
    anomaly_rate_pct,
    round(
        case
            when signal_score is not null then
                (signal_score * 0.40) + (drop_score * 0.30) + (latency_score * 0.20) + (radio_score * 0.10)
            else
                radio_score
        end,
        2
    ) as health_score
from scored
order by health_score asc
