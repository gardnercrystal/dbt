-- add workspace records for EDL and DAP workspaces which are used internally by the development team
select 'EDL_DEV' workspace_code, 'EDL development' workspace_name, 'EDL development' workspace_desc, 'EDL' project_code, 'Enterprise Data Lake' project_name, 'EDL platform development, maintenance and BAU' project_desc, 'DWBI' customer_name, false billable_flg, '2022-07-01T00:00:00Z'::timestamp_ntz billing_start_dt, '9999-12-31T00:00:00Z'::timestamp_ntz billing_end_dt, null bill_to_cost_center, null bill_to_otl_code, false delete_flg,  '2022-07-01T00:00:00Z'::timestamp_ntz effective_from_dt, '9999-12-31T00:00:00Z'::timestamp_ntz effective_to_dt union all
select 'EDL_PREPROD', 'EDL pre-production', 'EDL pre-production', 'EDL', 'Enterprise Data Lake', 'EDL platform development, maintenance and BAU', 'DWBI', false, '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
select 'EDL_PROD', 'EDL production', 'EDL production', 'EDL', 'Enterprise Data Lake', 'EDL platform development, maintenance and BAU', 'DWBI', false, '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
select 'DAP_DEV', 'DAP development', 'DAP development', 'DAP', 'Data Analytics Platform', 'DAP platform development, maintenance and BAU', 'DWBI', false, '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
select 'DAP_PREPROD', 'DAP pre-production', 'DAP pre-production', 'DAP', 'Data Analytics Platform', 'DAP platform development, maintenance and BAU', 'DWBI', false, '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
select 'DAP_PROD', 'DAP production', 'DAP production', 'DAP', 'Data Analytics Platform', 'DAP platform development, maintenance and BAU', 'DWBI', false, '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
-- standard unspecified row, to help avoid outer joins with fact tables
select 'UNKNOWN', 'Unknown', null, 'UNKNOWN', 'Unknown', null, null, false, null, null, null, null, false,  '2022-07-01T00:00:00Z'::timestamp_ntz, '9999-12-31T00:00:00Z'::timestamp_ntz union all
select
  -- workspace attributes
  w.workspace_code as workspace_code,
  w.workspace_name as workspace_name,
  w.description as workspace_desc, -- workspace description
  -- project attributes - a project can have multiple workspaces
  p.project_code as project_code,
  p.project_name as project_name,
  p.project_description as project_desc,
  p.customer_name as customer_name,
  -- billing attributes - used for passing EDL costs to Apptio
  -- generally billing attributes are at project level, but can be at workspace level too
  p.bill_flag as billable_flg,
  p.valid_from::timestamp_ntz as billing_start_dt,
  p.valid_to::timestamp_ntz as billing_end_dt,
  p.cost_center as bill_to_cost_center,
  p.otl_code as bill_to_otl_code,
  -- standard dwh attributes
  false as delete_flg,
  -- generic SCD2 columns to track changes to data
  p.valid_from::timestamp_ntz as effective_from_dt,
  p.valid_to::timestamp_ntz as effective_to_dt
from
  {{ source('old_billing', 'edl_workspace') }} w
  inner join {{ source('old_billing', 'edl_project') }} p on p.project_id = w.project_id
where
  p.project_id not in (16, 19) -- edl and dap dev projects
