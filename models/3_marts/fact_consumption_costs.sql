{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    unique_key = ['workspace_id', 'usage_dt_id', 'warehouse_id'],
    merge_exclude_columns = ['insert_dt']
)}}

with rows_to_merge as (
  select
    coalesce(wrk.workspace_id, 0) as workspace_id,
    stg.usage_dt_id,
    stg.usage_dt,
    stg.warehouse_id,
    stg.warehouse_name,
    stg.comments,
    stg.compute_costs_usd,
    stg.cloud_services_costs_usd,
    stg.auto_clustering_costs_usd,
    stg.mv_refresh_costs_usd,
    stg.search_optimization_costs_usd,
    stg.database_replication_costs_usd,
    stg.query_acceleration_costs_usd,
    stg.serverless_task_costs_usd,
    stg.snowpipe_costs_usd,
    stg.other_consumption_costs_usd,
    stg.total_consumption_costs_usd,
    stg.delete_flg
  from
    {{ ref('stg_consumption_costs') }} stg

    left join {{ ref('dim_workspace') }} wrk on 
    wrk.workspace_code = stg.workspace_code and
    current_timestamp() between wrk.effective_from_dt and wrk.effective_to_dt

  -- run the following minus operation only during incrementals
  -- minus ensures the result set only contains new or changed rows
  {% if is_incremental() %}
  minus
  select 
    workspace_id,
    usage_dt_id,
    usage_dt,
    warehouse_id,
    warehouse_name,
    comments,
    compute_costs_usd,
    cloud_services_costs_usd,
    auto_clustering_costs_usd,
    mv_refresh_costs_usd,
    search_optimization_costs_usd,
    database_replication_costs_usd,
    query_acceleration_costs_usd,
    serverless_task_costs_usd,
    snowpipe_costs_usd,
    other_consumption_costs_usd,
    total_consumption_costs_usd,
    delete_flg 
  from 
    {{ this }}
  {% endif %}
)
select
  rows_to_merge.workspace_id as workspace_id,
  rows_to_merge.usage_dt_id as usage_dt_id, 
  rows_to_merge.usage_dt as usage_dt, 
  rows_to_merge.warehouse_id as warehouse_id, 
  rows_to_merge.warehouse_name as warehouse_name, 
  rows_to_merge.comments as comments, 
  rows_to_merge.compute_costs_usd as compute_costs_usd, 
  rows_to_merge.cloud_services_costs_usd as cloud_services_costs_usd, 
  rows_to_merge.auto_clustering_costs_usd as auto_clustering_costs_usd, 
  rows_to_merge.mv_refresh_costs_usd as mv_refresh_costs_usd,  
  rows_to_merge.search_optimization_costs_usd as search_optimization_costs_usd,  
  rows_to_merge.database_replication_costs_usd as database_replication_costs_usd, 
  rows_to_merge.query_acceleration_costs_usd as query_acceleration_costs_usd,
  rows_to_merge.serverless_task_costs_usd as serverless_task_costs_usd,  
  rows_to_merge.snowpipe_costs_usd as snowpipe_costs_usd, 
  rows_to_merge.other_consumption_costs_usd as other_consumption_costs_usd,
  rows_to_merge.total_consumption_costs_usd as total_consumption_costs_usd,
  rows_to_merge.delete_flg as delete_flg,
  current_timestamp()::timestamp_ntz as insert_dt,
  current_timestamp()::timestamp_ntz as update_dt
from
  rows_to_merge
