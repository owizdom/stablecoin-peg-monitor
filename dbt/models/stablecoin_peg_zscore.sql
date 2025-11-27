{{
    config(
        materialized='table'
    )
}}

-- Calculate 24h rolling statistics and z-score for peg deviation
with peg_health as (
    select * from {{ ref('stablecoin_peg_health') }}
),

rolling_stats as (
    select
        token,
        timestamp,
        price,
        deviation_from_peg,
        -- 24h rolling mean (assuming timestamps are in seconds, 24h = 86400 seconds)
        avg(deviation_from_peg) over (
            partition by token
            order by timestamp
            range between 86400 preceding and current row
        ) as rolling_mean_24h,
        -- 24h rolling standard deviation
        stddev_pop(deviation_from_peg) over (
            partition by token
            order by timestamp
            range between 86400 preceding and current row
        ) as rolling_stddev_24h,
        -- Count of observations in 24h window
        count(*) over (
            partition by token
            order by timestamp
            range between 86400 preceding and current row
        ) as observations_24h
    from peg_health
)

select
    token,
    timestamp,
    price,
    deviation_from_peg,
    rolling_mean_24h,
    rolling_stddev_24h,
    observations_24h,
    -- Calculate z-score: (deviation - mean) / stddev
    case
        when rolling_stddev_24h > 0 
        then (deviation_from_peg - rolling_mean_24h) / rolling_stddev_24h
        else null
    end as zscore,
    -- Z-score interpretation
    case
        when rolling_stddev_24h > 0 and abs((deviation_from_peg - rolling_mean_24h) / rolling_stddev_24h) > 2 then 'outlier'
        when rolling_stddev_24h > 0 and abs((deviation_from_peg - rolling_mean_24h) / rolling_stddev_24h) > 1 then 'unusual'
        else 'normal'
    end as zscore_status
from rolling_stats
where observations_24h >= 2  -- Need at least 2 observations for meaningful stats
order by token, timestamp desc

