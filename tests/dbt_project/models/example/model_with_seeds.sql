{{ config(materialized='table') }}

select *
from {{ ref('test_seed') }}