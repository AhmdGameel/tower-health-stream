with tower_health as (
    select * from {{ ref('tower_health_scores') }}
),

operator_stats as (
    select
        operator,
        count(*)                                                        as total_towers,
        sum(case when radio = 'LTE'  then 1 else 0 end)                 as lte_towers,
        sum(case when radio = 'UMTS' then 1 else 0 end)                 as umts_towers,
        sum(case when radio = 'GSM'  then 1 else 0 end)                 as gsm_towers,
        round(sum(case when radio = 'LTE'  then 1 else 0 end) * 100.0 / count(*), 2) as lte_pct,
        round(sum(case when radio = 'UMTS' then 1 else 0 end) * 100.0 / count(*), 2) as umts_pct,
        round(sum(case when radio = 'GSM'  then 1 else 0 end) * 100.0 / count(*), 2) as gsm_pct,
        count(distinct area)                                            as areas_covered,
        sum(event_count)                                                as telemetry_events,
        round(avg(avg_signal_dbm), 2)                                   as avg_signal_dbm,
        round(avg(avg_latency_ms), 2)                                   as avg_latency_ms,
        round(avg(avg_drop_rate_pct), 2)                                as avg_drop_rate_pct,
        round(avg(anomaly_rate_pct), 2)                                 as avg_anomaly_rate_pct,
        round(avg(health_score), 2)                                     as avg_health_score
    from tower_health
    group by operator
)

select
    operator,
    total_towers,
    lte_towers,
    umts_towers,
    gsm_towers,
    lte_pct,
    umts_pct,
    gsm_pct,
    areas_covered,
    telemetry_events,
    avg_signal_dbm,
    avg_latency_ms,
    avg_drop_rate_pct,
    avg_anomaly_rate_pct,
    avg_health_score,
    rank() over (order by avg_health_score desc) as performance_rank
from operator_stats
order by performance_rank
