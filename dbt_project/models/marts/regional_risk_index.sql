with tower_health as (
    select * from {{ ref('tower_health_scores') }}
),

area_stats as (
    select
        area,
        count(*)                                                                     as total_towers,
        sum(case when radio = 'LTE'  then 1 else 0 end)                              as lte_count,
        sum(case when radio = 'UMTS' then 1 else 0 end)                              as umts_count,
        sum(case when radio = 'GSM'  then 1 else 0 end)                              as gsm_count,
        round(sum(case when radio = 'LTE' then 1 else 0 end) * 100.0 / count(*), 2)  as lte_coverage_pct,
        sum(event_count)                                                             as observed_events,
        round(avg(avg_signal_dbm), 2)                                                as avg_signal_dbm,
        round(avg(anomaly_rate_pct), 2)                                              as avg_anomaly_rate_pct,
        round(avg(health_score), 2)                                                  as avg_health_score
    from tower_health
    group by area
),

risk_scored as (
    select
        area,
        total_towers,
        lte_count,
        umts_count,
        gsm_count,
        lte_coverage_pct,
        observed_events,
        avg_signal_dbm,
        avg_anomaly_rate_pct,
        avg_health_score,
        -- Combine infrastructure risk (non-LTE %) with observed health degradation
        round(((100.0 - lte_coverage_pct) * 0.4) + ((100.0 - avg_health_score) * 0.6), 2) as risk_score
    from area_stats
)

select
    area,
    total_towers,
    lte_count,
    umts_count,
    gsm_count,
    lte_coverage_pct,
    observed_events,
    avg_signal_dbm,
    avg_anomaly_rate_pct,
    avg_health_score,
    risk_score,
    case
        when risk_score >= 55 then 'HIGH'
        when risk_score >= 30 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from risk_scored
order by risk_score desc
