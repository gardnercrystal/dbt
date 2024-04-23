{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    unique_key = ['workspace_id', 'usage_dt_id', 'table_id']
)}}

select
  -- foreign keys to dimension tables
  coalesce(wrk.workspace_id, 0) as workspace_id,
  stg.usage_dt_id as usage_dt_id,
  stg.usage_dt as usage_dt,
  -- table attributes
  stg.database_id as database_id,
  stg.database_name as database_name,
  stg.schema_id as schema_id,
  stg.schema_name as schema_name,
  stg.table_id as table_id,
  stg.table_name as table_name,
  stg.is_transient_flg as is_transient_flg,
  stg.is_deleted_flg as is_deleted_flg,
  stg.comments as comments,
  -- storage attributes
  stg.active_bytes as active_bytes,
  stg.time_travel_bytes as time_travel_bytes,
  stg.failsafe_bytes as failsafe_bytes,
  stg.retained_for_clone_bytes as retained_for_clone_bytes,
  stg.total_bytes as total_bytes,
  stg.total_storage_costs_usd as total_storage_costs_usd,
  -- standard dwh attributes
  stg.delete_flg as delete_flg,
  current_timestamp()::timestamp_ntz as insert_dt,
  current_timestamp()::timestamp_ntz as update_dt
from
  {{ ref('stg_storage_costs') }} stg

  left join {{ ref('dim_workspace') }} wrk on 
  wrk.workspace_code = stg.workspace_code and
  current_timestamp() between wrk.effective_from_dt and wrk.effective_to_dt
