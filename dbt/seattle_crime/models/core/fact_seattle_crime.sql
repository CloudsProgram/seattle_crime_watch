/*
    create fact table for seattle crime data
*/


{{ config(
    materialized='table',
    partition_by={"field":"report_datetime","data_type":"timestamp","granularity":"month"},
    cluster_by=["area"],

) }}


SELECT * FROM {{ ref('stg_seattle_crime') }} 

