{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    unique_key='cal_dt_id'
)}}

-- Calculate number of days to generate dates using input parameters start_date and end_date
{% set diff_days = (modules.datetime.datetime.strptime(var("end_date"), "%Y-%m-%d") - modules.datetime.datetime.strptime(var("start_date"), "%Y-%m-%d")).days %}

select
  -- day attributes
  dateadd(day, row_number() over (order by null), to_date('{{ var("start_date") }}')) as cal_dt,
  to_number(to_char(cal_dt, 'yyyymmdd')) as cal_dt_id,
  to_timestamp(cal_dt) as cal_dt_time_start,
  dateadd(millisecond, 86399999, cal_dt_time_start) as cal_dt_time_end,
  dayname(cal_dt) as cal_day,
  -- month attributes
  to_char(cal_dt, 'mon-yyyy') as cal_month,
  to_number(to_char(date_trunc(month, cal_dt_time_start), 'yyyymmdd')) as cal_month_id,
  monthname(cal_dt) as month_name,
  date_part(month, cal_dt) as month_number,
  date_trunc(month, cal_dt_time_start) as month_start_dt,
  dateadd(millisecond, 86399999, last_day(cal_dt, month)) as month_end_dt,
  -- quarter attributes
  date_part(year, cal_dt)||' Q'||date_part(quarter, cal_dt) as cal_qtr,
  to_number(to_char(date_trunc(quarter, cal_dt_time_start), 'yyyymmdd')) as cal_qtr_id,
  date_part(quarter, cal_dt) as qtr_number,
  date_trunc(qtr, cal_dt_time_start) as qtr_start_dt,
  dateadd(millisecond, 86399999, last_day(cal_dt, quarter)) as qtr_end_dt,
  -- year attributes
  date_part(year, cal_dt) as cal_year,
  date_trunc(year, cal_dt_time_start) as year_start_dt,
  dateadd(millisecond, 86399999, last_day(cal_dt, year)) as year_end_dt
from
  table(generator(rowcount => {{diff_days}}))

-- standard unspecified row, to help avoid outer joins with fact tables
union all
select
  to_date('1900-01-01', 'yyyy-mm-dd') as cal_dt,
  0 as cal_dt_id,
  null as cal_dt_time_start,
  null as cal_dt_time_end,
  null as cal_day,
  null as cal_month,
  null as cal_month_id,
  null as month_name,
  null as month_number,
  null as month_start_dt,
  null as month_end_dt,
  null as cal_qtr,
  null as cal_qtr_id,
  null as qtr_number,
  null as qtr_start_dt,
  null as qtr_end_dt,
  null as cal_year,
  null as year_start_dt,
  null as year_end_dt

-- run the following minus operation only during incrementals
-- minus ensures the result set only contains new or changed rows
{% if is_incremental() %}
minus
select
  cal_dt,
  cal_dt_id,
  cal_dt_time_start,
  cal_dt_time_end,
  cal_day,
  cal_month,
  cal_month_id,
  month_name,
  month_number,
  month_start_dt,
  month_end_dt,
  cal_qtr,
  cal_qtr_id,
  qtr_number,
  qtr_start_dt,
  qtr_end_dt,
  cal_year,
  year_start_dt,
  year_end_dt
from
  {{ this }}
{% endif %}
