-- models/marts/dim_date.sql
{{ config(materialized='table', tags=['dim', 'date']) }}

with src as (
    -- Parse timestamps from the source; ignore header-like rows or bad values
    select
        to_timestamp_ntz(started_at) as started_at
    from {{ source('demo', 'bike') }}
    where try_to_timestamp_ntz(started_at) is not null
),

bounds as (
    select
        date_trunc('day', min(started_at)) as min_day,
        date_trunc('day', max(started_at)) as max_day
    from src
),

span as (
    select datediff('day', min_day, max_day) + 1 as day_count
    from bounds
),

series as (
    -- Build a continuous series of days from min → max
    select
        dateadd('day', seq4(), (select min_day from bounds))::date as date_day
    from table(generator(rowcount => (select day_count from span)))
),

dim as (
    select
        /* Keys */
        date_day                                              as date,
        to_char(date_day, 'YYYYMMDD')::integer                as date_key,

        /* Calendars */
        year(date_day)                                        as year,
        quarter(date_day)                                     as quarter,
        concat('Q', quarter(date_day))                        as quarter_name,
        month(date_day)                                       as month,
        to_char(date_day, 'Month')                            as month_name,
        to_char(date_day, 'Mon')                              as month_name_short,
        day(date_day)                                         as day_of_month,
        dayofweekiso(date_day)                                as day_of_week_iso,   -- Mon=1..Sun=7
        to_char(date_day, 'Day')                              as day_name,
        to_char(date_day, 'DY')                               as day_name_short,
        week(date_day)                                        as week_of_year,
        weekiso(date_day)                                     as iso_week_of_year,
        date_trunc('week',    date_day)                       as week_start_date,
        date_trunc('month',   date_day)                       as month_start_date,
        date_trunc('quarter', date_day)                       as quarter_start_date,
        date_trunc('year',    date_day)                       as year_start_date,
        last_day(date_day, 'month')                           as month_end_date,
        last_day(date_day, 'year')                            as year_end_date,

        /* Business flags (mirroring the screenshot logic) */
        case when dayofweekiso(date_day) in (6, 7)
             then 'WEEKEND' else 'BUSINESSDAY' end           as day_type,
        case
            when month(date_day) in (12, 1, 2) then 'WINTER'
            when month(date_day) in (3,  4, 5) then 'SPRING'
            when month(date_day) in (6,  7, 8) then 'SUMMER'
            else 'AUTUMN'
        end                                                   as season_of_year,

        /* “Current” helpers */
        case when date_day = current_date then true else false end as is_current_day,
        case when year(date_day) = year(current_date)
               and month(date_day) = month(current_date)
             then true else false end                         as is_current_month,
        case when year(date_day) = year(current_date)
             then true else false end                         as is_current_year
    from series
)

select *
from dim
order by date;
