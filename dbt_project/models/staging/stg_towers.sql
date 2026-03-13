with raw_towers as (
    select *
    from read_csv_auto('{{ env_var("TOWERS_CSV_PATH", "/app/data/towers_clean.csv") }}')
)

select
    cast(tower_id as bigint) as tower_id,
    trim(operator)           as operator,
    upper(trim(radio))       as radio,
    cast(lat as double)      as lat,
    cast(lon as double)      as lon,
    cast(range as bigint)    as range_m,
    cast(area as bigint)     as area
from raw_towers
where tower_id is not null
  and lat is not null
  and lon is not null
qualify row_number() over (partition by tower_id order by range_m desc) = 1
