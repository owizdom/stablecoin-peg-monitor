{{
    config(
        materialized='view'
    )
}}

-- Calculate peg health metrics from Chainlink prices
select
    timestamp,
    token,
    price,
    price - 1.0 as deviation_from_peg,
    abs(price - 1.0) as abs_deviation_from_peg,
    (price - 1.0) * 100 as deviation_from_peg_pct,
    case
        when abs(price - 1.0) <= 0.001 then 'healthy'  -- Within 0.1%
        when abs(price - 1.0) <= 0.005 then 'warning'  -- Within 0.5%
        else 'critical'  -- > 0.5%
    end as peg_status,
    loaded_at
from {{ source('snowflake_raw', 'CHAINLINK_PRICES') }}
order by token, timestamp desc

