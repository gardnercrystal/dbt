select
  -- foreign keys to dimension tables
  coalesce(workspace_code, 'UNKNOWN') as workspace_code,
  usage_dt_id,
  usage_dt,
  -- consumption attributes
  warehouse_id,
  warehouse_name,
  null as comments,
  -- consumption costs
  round(sum(compute_costs_usd), 5) as compute_costs_usd,
  round(sum(cloud_services_costs_usd), 5) as cloud_services_costs_usd,
  round(sum(auto_clustering_costs_usd), 5) as auto_clustering_costs_usd,
  round(sum(mv_refresh_costs_usd), 5) as mv_refresh_costs_usd,
  round(sum(search_optimization_costs_usd), 5) as search_optimization_costs_usd,
  round(sum(database_replication_costs_usd), 5) as database_replication_costs_usd,
  round(sum(query_acceleration_costs_usd), 5) as query_acceleration_costs_usd,
  round(sum(serverless_task_costs_usd), 5) as serverless_task_costs_usd,
  round(sum(snowpipe_costs_usd), 5) as snowpipe_costs_usd,
  0 as other_consumption_costs_usd,
  round(sum(compute_costs_usd + cloud_services_costs_usd + auto_clustering_costs_usd + mv_refresh_costs_usd + search_optimization_costs_usd + database_replication_costs_usd + query_acceleration_costs_usd + serverless_task_costs_usd + snowpipe_costs_usd + other_consumption_costs_usd), 5) as total_consumption_costs_usd,
  -- standard dwh attributes
  false as delete_flg
from
  (
    -- compute costs and cloud services costs from warehouse_metering_history
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(wmh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(wmh.start_time) as usage_dt,
      wmh.warehouse_id as warehouse_id,
      wmh.warehouse_name as warehouse_name,
      sum(wmh.credits_used_compute) * 5.5 as compute_costs_usd,
      sum(wmh.credits_used_cloud_services) * 5.5 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'warehouse_metering_history') }} wmh

      -- get warehouse tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = wmh.warehouse_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'WAREHOUSE'
    where
      wmh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(wmh.start_time, 'yyyymmdd'),
      to_date(wmh.start_time),
      wmh.warehouse_id,
      wmh.warehouse_name
    union all
    -- auto_clustering_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(ach.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(ach.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      sum(ach.credits_used) * 5.5 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'automatic_clustering_history') }} ach 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = ach.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' 
    where
      ach.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(ach.start_time, 'yyyymmdd'),
      to_date(ach.start_time)
    union all
    -- mv_refresh_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(mvr.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(mvr.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      sum(mvr.credits_used) * 5.5 as mv_refresh_costs_usd,  
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'materialized_view_refresh_history') }} mvr 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = mvr.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' 
    where
      mvr.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(mvr.start_time, 'yyyymmdd'),
      to_date(mvr.start_time)
    union all
    -- search_optimization_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(soh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(soh.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,  
      sum(soh.credits_used) * 5.5 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'search_optimization_history') }} soh 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = soh.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE'
    where
      soh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(soh.start_time, 'yyyymmdd'),
      to_date(soh.start_time)
    union all
    -- database_replication_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(drh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(drh.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,  
      0 as search_optimization_costs_usd,
      sum(drh.credits_used) * 5.5 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'database_replication_usage_history') }} drh 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = drh.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE'
    where
      drh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(drh.start_time, 'yyyymmdd'),
      to_date(drh.start_time)
    union all
    -- query_acceleration_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(qah.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(qah.start_time) as usage_dt,
      qah.warehouse_id as warehouse_id,
      qah.warehouse_name as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,  
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      sum(qah.credits_used) * 5.5 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'query_acceleration_history') }} qah 
      
      -- get warehouse tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = qah.warehouse_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'WAREHOUSE'
    where
      qah.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(qah.start_time, 'yyyymmdd'),
      to_date(qah.start_time),
      qah.warehouse_id,
      qah.warehouse_name
    union all
    -- serverless_task_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(sth.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(sth.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,  
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      sum(sth.credits_used) * 5.5 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'serverless_task_history') }} sth 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = sth.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' 
    where
      sth.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(sth.start_time, 'yyyymmdd'),
      to_date(sth.start_time)
    union all
    -- snowpipe_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      tag.tag_value as workspace_code,
      to_char(sfh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(sfh.start_time) as usage_dt,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,  
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      sum(sfh.credits_used) * 5.5 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'snowpipe_streaming_file_migration_history') }} sfh 
      
      -- get database tags
      left join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = sfh.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' 
    where
      sfh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      tag.tag_value,
      to_char(sfh.start_time, 'yyyymmdd'),
      to_date(sfh.start_time)
  )
/*where
  workspace_id not in (1, 2, 3)*/
group by 
  workspace_code,
  usage_dt_id,
  usage_dt,
  warehouse_id,
  warehouse_name
