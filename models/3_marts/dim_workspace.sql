{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    unique_key = ['workspace_code', 'effective_from_dt'],
    merge_exclude_columns = ['workspace_id', 'insert_dt']
)}}

with rows_to_merge as (
  select
    -- workspace attributes
    workspace_code,
    workspace_name,
    workspace_desc,
    -- project attributes - a project can have multiple workspaces - but mostly one
    project_code,
    project_name,
    project_desc,
    customer_name,
    -- billing attributes - used for passing EDL costs to Apptio
    -- generally billing attributes are at project level, but can be at workspace level too
    billable_flg,
    billing_start_dt,
    billing_end_dt,
    bill_to_cost_center,
    bill_to_otl_code,
    -- standard dwh attributes
    delete_flg,
    -- generic SCD2 columns to track changes to data
    effective_from_dt,
    effective_to_dt
  from
    {{ ref('stg_workspace') }}

  -- run the following minus operation only during incrementals
  -- minus ensures the result set only contains new or changed rows
  {% if is_incremental() %}
  minus
  select 
    workspace_code, 
    workspace_name, 
    workspace_desc, 
    project_code, 
    project_name, 
    project_desc, 
    customer_name, 
    billable_flg, 
    billing_start_dt, 
    billing_end_dt, 
    bill_to_cost_center,
    bill_to_otl_code, 
    delete_flg, 
    effective_from_dt, 
    effective_to_dt 
  from 
    {{ this }}
  {% endif %}
)
select
  case
    when rows_to_merge.workspace_code = 'UNKNOWN' then 0 
    else {{ increment_sequence() }}
  end as workspace_id,
  rows_to_merge.workspace_code as workspace_code, 
  rows_to_merge.workspace_name as workspace_name, 
  rows_to_merge.workspace_desc as workspace_desc, 
  rows_to_merge.project_code as project_code, 
  rows_to_merge.project_name as project_name, 
  rows_to_merge.project_desc as project_desc, 
  rows_to_merge.customer_name as customer_name, 
  rows_to_merge.billable_flg as billable_flg, 
  rows_to_merge.billing_start_dt as billing_start_dt,  
  rows_to_merge.billing_end_dt as billing_end_dt,  
  rows_to_merge.bill_to_cost_center as bill_to_cost_center, 
  rows_to_merge.bill_to_otl_code as bill_to_otl_code,  
  rows_to_merge.delete_flg as delete_flg,
  current_timestamp()::timestamp_ntz as insert_dt,
  current_timestamp()::timestamp_ntz as update_dt,
  rows_to_merge.effective_from_dt as effective_from_dt, 
  rows_to_merge.effective_to_dt as effective_to_dt
from
  rows_to_merge
