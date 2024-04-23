{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    unique_key = ['workspace_code', 'subscription_start_dt', 'source_system_name', 'source_instance_name', 'source_schema_name', 'source_table_name', 'effective_from_dt'],
    merge_exclude_columns = ['subscription_id', 'insert_dt']
)}}

with rows_to_merge as (
  select
    case when project_code = 'NSLHD_CQR' then p.project_code|| '_DEV' else p.project_code end as workspace_code,
    s.valid_from::timestamp_ntz as subscription_start_dt,
    s.valid_to::timestamp_ntz as subscription_end_dt,
    -- source details
    t.system_name as source_system_name,
    t.instance_name as source_instance_name,
    t.schema_name as source_schema_name,
    t.table_name as source_table_name,
    -- target details
    coalesce(st.table_catalog, 'EDL_'||upper(t.system_name)||'_'||upper(t.instance_name)) as target_database_name,
    coalesce(st.table_schema, upper(t.schema_name)) as target_schema_name,
    coalesce(st.table_name, upper(t.table_name)) as target_table_name,
    -- subscription details
    s.requested_freq as data_refresh_frequency_mins,
    false as realtime_flg,
    null as comments,
    -- standard dwh attributes
    false as delete_flg,
    -- generic SCD2 columns to track changes to data
    s.valid_from::timestamp_ntz as effective_from_dt,
    s.valid_to::timestamp_ntz as effective_to_dt
  from
    {{ source('old_billing', 'edl_subscription') }} s

    inner join {{ source('old_billing', 'edl_tables') }} t on 
    t.table_hash = s.table_hash and 
    t.valid_to > current_timestamp()

    inner join {{ source('old_billing', 'edl_project') }} p on 
    s.project_id = p.project_id

    left join {{ source('account_usage', 'tables') }} st on 
    upper(t.table_name) = st.table_name and 
    upper(t.schema_name) = st.table_schema and 
    'EDL_'||upper(t.system_name)||'_'||upper(t.instance_name) = st.table_catalog and 
    st.deleted is null
  where
    -- only bring current subscriptions
    s.valid_to > current_timestamp()

  -- run the following minus operation only during incrementals
  -- minus ensures the result set only contains new or changed rows
  {% if is_incremental() %}
  minus
  select
    workspace_code,
    subscription_start_dt,
    subscription_end_dt,
    source_system_name,
    source_instance_name,
    source_schema_name,
    source_table_name,
    target_database_name,
    target_schema_name,
    target_table_name,
    data_refresh_frequency_mins,
    realtime_flg,
    comments,
    delete_flg,
    effective_from_dt,
    effective_to_dt
  from 
    {{ this }}
  {% endif %}
)
select
  {{ increment_sequence() }} as subscription_id,
  rows_to_merge.workspace_code as workspace_code,
  rows_to_merge.subscription_start_dt as subscription_start_dt,
  rows_to_merge.subscription_end_dt as subscription_end_dt,
  rows_to_merge.source_system_name as source_system_name,
  rows_to_merge.source_instance_name as source_instance_name,
  rows_to_merge.source_schema_name as source_schema_name,
  rows_to_merge.source_table_name as source_table_name,
  rows_to_merge.target_database_name as target_database_name,
  rows_to_merge.target_schema_name as target_schema_name,
  rows_to_merge.target_table_name as target_table_name,
  rows_to_merge.data_refresh_frequency_mins as data_refresh_frequency_mins,
  rows_to_merge.realtime_flg as realtime_flg,
  rows_to_merge.comments as comments,
  rows_to_merge.delete_flg as delete_flg,
  current_timestamp()::timestamp_ntz as insert_dt,
  current_timestamp()::timestamp_ntz as update_dt,
  rows_to_merge.effective_from_dt as effective_from_dt, 
  rows_to_merge.effective_to_dt as effective_to_dt
from
  rows_to_merge
