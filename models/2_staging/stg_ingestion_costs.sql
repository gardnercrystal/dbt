with query_history as 
(
  -- the objective is to join query_history with warehouse_metering_history to get query level cost
  -- however a query's compute costs are often found in the following hour in warehouse_metering_history
  -- the only reliable way to join these two tables is at a day level.
  -- the following query aggregates query_history at day level
  select
    dt.cal_dt as usage_dt,
    qh.warehouse_id as warehouse_id,
    qh.warehouse_name as warehouse_name,
    t.table_catalog as table_catalog,
    t.table_schema as table_schema,
    t.table_name as table_name,
    try_parse_json(qh.query_tag):table_name::varchar as full_table_name,
    lower(try_parse_json(qh.query_tag):purpose::varchar) as ingestion_type,
    -- get the sum of execution time for this table
    -- a query could run across multiple dates, so calculate execution time per day
    sum ( 
      case
        when dateadd(millisecond, -qh.execution_time, qh.end_time) > dt.cal_dt_time_end then 0
        else timestampdiff(millisecond, greatest(dateadd(millisecond, -qh.execution_time, qh.end_time), dt.cal_dt_time_start), least(qh.end_time, dt.cal_dt_time_end))
      end
    ) as trimmed_execution_time,
    -- total_wh_up_time is the total time the warehouse was up incurring charges
    sum(trimmed_execution_time) over (partition by dt.cal_dt, qh.warehouse_id) as total_wh_up_time,
    -- table_share_of_wh_up_time is the table's share of total_wh_up_time, i.e. the time warehouse was up loading the table
    case when total_wh_up_time = 0 then 0 else trimmed_execution_time / total_wh_up_time end as table_share_of_wh_up_time
  from
    {{ source('account_usage', 'query_history') }} qh
    
    -- join with tables to get correct database and schema information
    inner join {{ source('account_usage', 'tables') }} t on 
    try_parse_json(qh.query_tag):table_name::varchar = t.table_catalog||'.'||t.table_schema||'.'||t.table_name and
    qh.start_time between t.created and coalesce(t.deleted, current_timestamp())

    -- join with date table to get usage per day
    inner join {{ ref('dim_date') }} dt on 
    dt.cal_dt between to_date(qh.start_time) and to_date(qh.end_time)

  where 
    regexp_instr(qh.query_tag, 'ingestion_flg') > 0 and -- filter only ingestion queries
    qh.warehouse_size is not null and -- filter only queries that used a warehouse
    qh.start_time >= '2023-07-01'::timestamp_ntz -- data is only consistent after 1st July 2023
  group by
    dt.cal_dt,
    qh.warehouse_id,
    qh.warehouse_name,
    t.table_catalog,
    t.table_schema,
    t.table_name,
    try_parse_json(qh.query_tag):table_name::varchar,
    try_parse_json(qh.query_tag):purpose::varchar
),
warehouse_metering_history as 
(
  -- the following query aggregates warehouse_metering_history at day level
  select
    to_date(wmh.start_time) as usage_dt,
    wmh.warehouse_id as warehouse_id,
    wmh.warehouse_name as warehouse_name,
    -- unit cost of 1 snowflake credit is USD 5.5
    sum(wmh.credits_used_compute) * 5.5 as compute_costs_usd,
    sum(wmh.credits_used_cloud_services) * 5.5 as cloud_services_costs_usd
  from
    {{ source('account_usage', 'warehouse_metering_history') }} wmh
  where
    -- data consistent only from 1st July 2023
    wmh.start_time >= '2023-07-01'::timestamp_ntz
  group by 
    to_date(wmh.start_time),
    wmh.warehouse_id,
    wmh.warehouse_name
),
query_history_with_subscription as 
(
  select 
    sub.workspace_code as workspace_code,
    qh.usage_dt as usage_dt,
    qh.table_catalog as table_catalog,
    qh.table_schema as table_schema,
    qh.table_name as table_name,
    sub.source_system_name||'.'||sub.source_instance_name||'.'||sub.source_schema_name||'.'||sub.source_table_name as source_table_name,
    sub.data_refresh_frequency_mins as refresh_frequency,
    qh.ingestion_type as ingestion_type,
    qh.warehouse_id as warehouse_id,
    qh.warehouse_name as warehouse_name,
    -- temp calculated columns to help with final calculation
    -- counting the number of distinct workspaces that subscribed to the table
    count(distinct sub.workspace_code) over (partition by qh.full_table_name, qh.usage_dt) as number_of_workspaces_subscribed_to_table,
    -- equitably distribute costs based on subscription frequency
    case when sub.data_refresh_frequency_mins = 0 then 0 else (1 / sub.data_refresh_frequency_mins) end as refresh_frequency_mins_inverted,
    sum(refresh_frequency_mins_inverted) over (partition by qh.full_table_name, qh.usage_dt, qh.ingestion_type) as total_refresh_frequency_mins_inverted,
    case
      -- split ingestion costs equally among all subscribed workspaces for bulk, catchup, ddl changes, blob stitch and near realtime loads
      when 
        sub.realtime_flg = 'true' or 
        qh.ingestion_type in ('batch_bulk', 'batch_catchup', 'batch_ddl', 'blob_stitch_merge', 'near_realtime')
      then
        case when number_of_workspaces_subscribed_to_table = 0 then 0 else 1 / number_of_workspaces_subscribed_to_table end
      -- split ingestion costs based on frequency for incremental loads
      when 
        qh.ingestion_type in ('batch_incremental', 'dbt_transform', 'fif_load', 'fif_metadataload')
      then
        case when total_refresh_frequency_mins_inverted = 0 then 0 else refresh_frequency_mins_inverted / total_refresh_frequency_mins_inverted end
      else 0
    end as workspace_table_ratio_per_subscription,
    qh.table_share_of_wh_up_time
  from 
    query_history qh

    left join {{ ref('meta_subscriptions') }} sub on 
    sub.target_database_name = qh.table_catalog and
    sub.target_schema_name = qh.table_schema and
    (
      sub.target_table_name = qh.table_name or
      'TEMP_LOADING_TABLE_'||sub.target_table_name = qh.table_name or
      'hot_'||sub.target_table_name = qh.table_name or 
      'stitch_'||sub.target_table_name = qh.table_name or
      'temp_loading_stitch_'||sub.target_table_name = qh.table_name
    ) and
    qh.usage_dt between sub.subscription_start_dt and sub.subscription_end_dt and
    current_timestamp() between sub.effective_from_dt and sub.effective_to_dt
)
select
  -- foreign keys to dimension tables
  coalesce(workspace_code, 'UNKNOWN') as workspace_code,
  usage_dt_id,
  usage_dt,
  -- table attributes
  database_name,
  schema_name,
  table_name,
  source_table_name,
  refresh_frequency,
  ingestion_type,
  warehouse_id,
  warehouse_name,
  null as comments,
  -- ingestion costs
  round(sum(compute_costs_usd), 5) as compute_costs_usd,
  round(sum(cloud_services_costs_usd), 5) as cloud_services_costs_usd,
  round(sum(auto_clustering_costs_usd), 5) as auto_clustering_costs_usd,
  round(sum(mv_refresh_costs_usd), 5) as mv_refresh_costs_usd,
  round(sum(search_optimization_costs_usd), 5) as search_optimization_costs_usd,
  round(sum(database_replication_costs_usd), 5) as database_replication_costs_usd, -- will always be zero because data is unavailable to calculate at a table level
  round(sum(query_acceleration_costs_usd), 5) as query_acceleration_costs_usd, -- will always be zero because data is unavailable to calculate at a table level
  round(sum(serverless_task_costs_usd), 5) as serverless_task_costs_usd, -- will always be zero because data is unavailable to calculate at a table level
  round(sum(snowpipe_costs_usd), 5) as snowpipe_costs_usd,
  0 as other_ingestion_costs_usd,
  round(sum(compute_costs_usd + cloud_services_costs_usd + auto_clustering_costs_usd + mv_refresh_costs_usd + search_optimization_costs_usd + database_replication_costs_usd + query_acceleration_costs_usd + serverless_task_costs_usd + snowpipe_costs_usd + other_ingestion_costs_usd), 5) as total_ingestion_costs_usd,
  -- standard dwh attributes
  false as delete_flg
from
  (
    -- compute costs and cloud services costs from warehouse_metering_history
    -- query_history and meta_subscriptions help to distribute costs across workspaces
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      qh.workspace_code as workspace_code,
      to_char(wmh.usage_dt, 'yyyymmdd')::number as usage_dt_id,
      wmh.usage_dt as usage_dt,
      qh.table_catalog as database_name,
      qh.table_schema as schema_name,
      qh.table_name as table_name,
      qh.source_table_name as source_table_name,
      qh.refresh_frequency as refresh_frequency,
      qh.ingestion_type as ingestion_type,
      wmh.warehouse_id as warehouse_id,
      wmh.warehouse_name as warehouse_name,
      -- workspace level compute costs are simply table_level_compute_costs_usd * workspace_table_ratio_per_subscription
      wmh.compute_costs_usd * qh.table_share_of_wh_up_time * qh.workspace_table_ratio_per_subscription as compute_costs_usd,
      wmh.cloud_services_costs_usd * qh.table_share_of_wh_up_time * qh.workspace_table_ratio_per_subscription as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      warehouse_metering_history wmh

      inner join query_history_with_subscription qh on 
      qh.warehouse_id = wmh.warehouse_id and
      qh.usage_dt = wmh.usage_dt

    union all
    -- auto_clustering_costs are divided equally among all subscribed workspaces
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      sub.workspace_code as workspace_code,
      to_char(ach.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(ach.start_time) as usage_dt,
      ach.database_name as database_name,
      ach.schema_name as schema_name,
      ach.table_name as table_name,
      'Not applicable' as source_table_name,
      null as refresh_frequency,
      'Not applicable' as ingestion_type,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      -- divide costs equally among all subscribed workspaces
      -- if no workspace subscription is available, then return the whole costs
      sum(ach.credits_used) * 5.5 / 
      case 
        when count(distinct sub.workspace_code) over (partition by to_date(ach.start_time), ach.database_name, ach.schema_name, ach.table_name) = 0 then 1 
        else count(distinct sub.workspace_code) over (partition by to_date(ach.start_time), ach.database_name, ach.schema_name, ach.table_name) 
      end as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'automatic_clustering_history') }} ach 
      
      -- only fetch clustering costs for EDL tables - clustering costs for customer tables are included in the consumption costs
      inner join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = ach.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' and 
      tag.tag_value in ('EDL_DEV', 'EDL_PREPROD', 'EDL_PROD')

      left join {{ ref('meta_subscriptions') }} sub on 
      sub.target_database_name = ach.database_name and
      sub.target_schema_name = ach.schema_name and
      (
        sub.target_table_name = ach.table_name or
        'TEMP_LOADING_TABLE_'||sub.target_table_name = ach.table_name or
        'hot_'||sub.target_table_name = ach.table_name or 
        'stitch_'||sub.target_table_name = ach.table_name or
        'temp_loading_stitch_'||sub.target_table_name = ach.table_name
      ) and
      ach.start_time between sub.subscription_start_dt and sub.subscription_end_dt and
      current_timestamp() between sub.effective_from_dt and sub.effective_to_dt
    where
      ach.start_time >= '2023-07-01'::timestamp_ntz
    group by
      sub.workspace_code,
      to_char(ach.start_time, 'yyyymmdd')::number,
      to_date(ach.start_time),
      ach.database_name,
      ach.schema_name,
      ach.table_name

    union all
    -- mv_refresh_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      sub.workspace_code as workspace_code,
      to_char(mvr.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(mvr.start_time) as usage_dt,
      mvr.database_name as database_name,
      mvr.schema_name as schema_name,
      mvr.table_name as table_name,
      'Not applicable' as source_table_name,
      null as refresh_frequency,
      'Not applicable' as ingestion_type,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      sum(mvr.credits_used) * 5.5 /
      case 
        when count(distinct sub.workspace_code) over (partition by to_date(mvr.start_time), mvr.database_name, mvr.schema_name, mvr.table_name) = 0 then 1
        else count(distinct sub.workspace_code) over (partition by to_date(mvr.start_time), mvr.database_name, mvr.schema_name, mvr.table_name) 
      end as mv_refresh_costs_usd,
      0 as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'materialized_view_refresh_history') }} mvr 
      
      -- only fetch mv refresh costs for EDL tables - costs for customer mv refreshes are included in the consumption costs
      inner join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = mvr.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' and 
      tag.tag_value in ('EDL_DEV', 'EDL_PREPROD', 'EDL_PROD')

      left join {{ ref('meta_subscriptions') }} sub on 
      sub.target_database_name = mvr.database_name and
      sub.target_schema_name = mvr.schema_name and
      (
        sub.target_table_name = mvr.table_name or
        'TEMP_LOADING_TABLE_'||sub.target_table_name = mvr.table_name or
        'hot_'||sub.target_table_name = mvr.table_name or 
        'stitch_'||sub.target_table_name = mvr.table_name or
        'temp_loading_stitch_'||sub.target_table_name = mvr.table_name
      ) and
      mvr.start_time between sub.subscription_start_dt and sub.subscription_end_dt and
      current_timestamp() between sub.effective_from_dt and sub.effective_to_dt
    where
      mvr.start_time >= '2023-07-01'::timestamp_ntz
    group by
      sub.workspace_code,
      to_char(mvr.start_time, 'yyyymmdd')::number,
      to_date(mvr.start_time),
      mvr.database_name,
      mvr.schema_name,
      mvr.table_name

    union all
    -- search_optimization_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      sub.workspace_code as workspace_code,
      to_char(soh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(soh.start_time) as usage_dt,
      soh.database_name as database_name,
      soh.schema_name as schema_name,
      soh.table_name as table_name,
      'Not applicable' as source_table_name,
      null as refresh_frequency,
      'Not applicable' as ingestion_type,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      sum(soh.credits_used) * 5.5 /
      case 
        when count(distinct sub.workspace_code) over (partition by to_date(soh.start_time), soh.database_name, soh.schema_name, soh.table_name) = 0 then 1
        else count(distinct sub.workspace_code) over (partition by to_date(soh.start_time), soh.database_name, soh.schema_name, soh.table_name) 
      end as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'search_optimization_history') }} soh 
      
      -- only fetch costs for EDL tables - costs for customer workspace tables are included in the consumption costs
      inner join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = soh.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' and 
      tag.tag_value in ('EDL_DEV', 'EDL_PREPROD', 'EDL_PROD')

      left join {{ ref('meta_subscriptions') }} sub on 
      sub.target_database_name = soh.database_name and
      sub.target_schema_name = soh.schema_name and
      (
        sub.target_table_name = soh.table_name or
        'TEMP_LOADING_TABLE_'||sub.target_table_name = soh.table_name or
        'hot_'||sub.target_table_name = soh.table_name or 
        'stitch_'||sub.target_table_name = soh.table_name or
        'temp_loading_stitch_'||sub.target_table_name = soh.table_name
      ) and
      soh.start_time between sub.subscription_start_dt and sub.subscription_end_dt and
      current_timestamp() between sub.effective_from_dt and sub.effective_to_dt
    where
      soh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      sub.workspace_code,
      to_char(soh.start_time, 'yyyymmdd')::number,
      to_date(soh.start_time),
      soh.database_name,
      soh.schema_name,
      soh.table_name

    union all
    -- snowpipe_costs
    -- unit cost of 1 snowflake credit is USD 5.5
    select 
      sub.workspace_code as workspace_code,
      to_char(sfh.start_time, 'yyyymmdd')::number as usage_dt_id,
      to_date(sfh.start_time) as usage_dt,
      sfh.database_name as database_name,
      sfh.schema_name as schema_name,
      sfh.table_name as table_name,
      'Not applicable' as source_table_name,
      null as refresh_frequency,
      'Not applicable' as ingestion_type,
      -1 as warehouse_id,
      'SNOWFLAKE_INTERNAL' as warehouse_name,
      0 as compute_costs_usd,
      0 as cloud_services_costs_usd,
      0 as auto_clustering_costs_usd,
      0 as mv_refresh_costs_usd,
      sum(sfh.credits_used) * 5.5 /
      case 
        when count(distinct sub.workspace_code) over (partition by to_date(sfh.start_time), sfh.database_name, sfh.schema_name, sfh.table_name) = 0 then 1
        else count(distinct sub.workspace_code) over (partition by to_date(sfh.start_time), sfh.database_name, sfh.schema_name, sfh.table_name) 
      end as search_optimization_costs_usd,
      0 as database_replication_costs_usd,
      0 as query_acceleration_costs_usd,
      0 as serverless_task_costs_usd,
      0 as snowpipe_costs_usd
    from 
      {{ source('account_usage', 'snowpipe_streaming_file_migration_history') }} sfh 
      
      -- only fetch costs for EDL tables - costs for customer workspace tables are included in the consumption costs
      inner join {{ source('account_usage', 'tag_references') }} tag on 
      tag.object_name = sfh.database_name and 
      tag.tag_database = 'EDL_GOVERNANCE' and
      tag.tag_schema = 'TAGS' and 
      tag.tag_name = 'WORKSPACE_CODE' and
      tag.domain = 'DATABASE' and 
      tag.tag_value in ('EDL_DEV', 'EDL_PREPROD', 'EDL_PROD')

      left join {{ ref('meta_subscriptions') }} sub on 
      sub.target_database_name = sfh.database_name and
      sub.target_schema_name = sfh.schema_name and
      (
        sub.target_table_name = sfh.table_name or
        'TEMP_LOADING_TABLE_'||sub.target_table_name = sfh.table_name or
        'hot_'||sub.target_table_name = sfh.table_name or 
        'stitch_'||sub.target_table_name = sfh.table_name or
        'temp_loading_stitch_'||sub.target_table_name = sfh.table_name
      ) and
      sfh.start_time between sub.subscription_start_dt and sub.subscription_end_dt and
      current_timestamp() between sub.effective_from_dt and sub.effective_to_dt
    where
      sfh.start_time >= '2023-07-01'::timestamp_ntz
    group by
      sub.workspace_code,
      to_char(sfh.start_time, 'yyyymmdd')::number,
      to_date(sfh.start_time),
      sfh.database_name,
      sfh.schema_name,
      sfh.table_name
  )
/*where
  workspace_id not in (1, 2, 3)*/
group by 
  workspace_code,
  usage_dt_id,
  usage_dt,
  database_name,
  schema_name,
  table_name,
  source_table_name,
  refresh_frequency,
  ingestion_type,
  warehouse_id,
  warehouse_name
