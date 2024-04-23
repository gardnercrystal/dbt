select
  -- foreign keys to dimension tables
  -- prioritise table level tag over schema level tag over database level tag to get workspace code
  coalesce(sub.workspace_code, schema_tag.tag_value, db_tag.tag_value, 'UNKNOWN') as workspace_code,
  to_char(current_date(), 'yyyymmdd')::number as usage_dt_id,
  current_date() as usage_dt,
  -- table attributes - pick clone table attributes first if applicable
  coalesce(cln.table_catalog_id, tbl.table_catalog_id) as database_id,
  coalesce(cln.table_catalog, tbl.table_catalog) as database_name,
  coalesce(cln.table_schema_id, tbl.table_schema_id) as schema_id,
  coalesce(cln.table_schema, tbl.table_schema) as schema_name,
  coalesce(cln.id, tbl.id) as table_id,
  coalesce(cln.table_name, tbl.table_name) as table_name,
  decode(lower(tbl.is_transient), 'yes', 1, 'no', 0, null)::boolean as is_transient_flg,
  tbl.deleted as is_deleted_flg, -- deleted tables can incur storage costs too
  null as comments,
  -- storage attributes
  tbl.active_bytes,
  tbl.time_travel_bytes,
  tbl.failsafe_bytes,
  tbl.retained_for_clone_bytes,
  tbl.active_bytes + tbl.time_travel_bytes + tbl.failsafe_bytes + tbl.retained_for_clone_bytes as total_bytes,
  -- tables can be subscribed by multiple workspaces, so proportion storage costs based on subscription count
  -- monthly unit cost for 1 TB storage is USD 46, so divide 46 by 30 to get daily costs
  round(total_bytes * 46/(30 * 1000 * 1000 * 1000 * 1000 * case when sub.subscription_id is not null then count(distinct sub.subscription_id) over (partition by sub.target_database_name, sub.target_schema_name, sub.target_table_name) else 1 end), 5) as total_storage_costs_usd,
  -- standard dwh attributes
  false as delete_flg
from 
  {{ source('account_usage', 'table_storage_metrics') }} tbl
  
  -- clone tables
  left join 
  (
    select 
      * 
    from 
      {{ source('account_usage', 'table_storage_metrics') }} 
    where 
      table_catalog is not null
  ) cln on 
  cln.clone_group_id = tbl.clone_group_id and 
  tbl.table_catalog is null

  -- subscriptions table
  left join {{ ref('meta_subscriptions') }} sub on 
  sub.target_database_name = coalesce(cln.table_catalog, tbl.table_catalog) and
  sub.target_schema_name = coalesce(cln.table_schema, tbl.table_schema) and
  (
    sub.target_table_name = coalesce(cln.table_name, tbl.table_name) or
    -- include all temporary and known prefixes used in EDL
    'TEMP_LOADING_TABLE_'||sub.target_table_name = coalesce(cln.table_name, tbl.table_name) or
    'hot_'||sub.target_table_name = coalesce(cln.table_name, tbl.table_name) or 
    'stitch_'||sub.target_table_name = coalesce(cln.table_name, tbl.table_name) or
    'temp_loading_stitch_'||sub.target_table_name = coalesce(cln.table_name, tbl.table_name)
  ) and
  current_date() between sub.subscription_start_dt and sub.subscription_end_dt and
  current_timestamp() between sub.effective_from_dt and sub.effective_to_dt

  -- first join with workspace table using workspace_code from subscription table
  left join {{ ref('dim_workspace') }} wrk on 
  wrk.workspace_code = sub.workspace_code and
  current_timestamp() between wrk.effective_from_dt and wrk.effective_to_dt

  -- second join with workspace table using workspace_code from database object tag
  left join {{ source('account_usage', 'tag_references') }} db_tag on 
  db_tag.object_name = tbl.table_catalog and 
  db_tag.tag_database = 'EDL_GOVERNANCE' and
  db_tag.tag_schema = 'TAGS' and 
  db_tag.tag_name = 'WORKSPACE_CODE' and
  db_tag.domain = 'DATABASE' 
  
  left join {{ ref('dim_workspace') }} wrk_1 on 
  wrk_1.workspace_code = db_tag.tag_value and
  current_timestamp() between wrk_1.effective_from_dt and wrk_1.effective_to_dt

  -- left join to tags to get schema level tags and subsequently workspace_code for edl_fif 
  left join {{ source('account_usage', 'tag_references') }} schema_tag on 
  schema_tag.object_name = tbl.table_schema and 
  schema_tag.tag_database = 'EDL_GOVERNANCE' and
  schema_tag.tag_schema = 'TAGS' and 
  schema_tag.tag_name = 'WORKSPACE_CODE' and
  schema_tag.domain = 'SCHEMA' 
  
  left join {{ ref('dim_workspace') }} wrk_2 on 
  wrk_2.workspace_code = schema_tag.tag_value and
  current_timestamp() between wrk_2.effective_from_dt and wrk_2.effective_to_dt

where
  -- only bring tables which incur storage costs
  tbl.active_bytes > 0 or 
  tbl.time_travel_bytes > 0 or 
  tbl.failsafe_bytes > 0 or 
  tbl.retained_for_clone_bytes > 0
