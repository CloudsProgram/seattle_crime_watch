/*
    This stages seattle crime data
*/


{{ config( materialized='view') }}


WITH vio_or_prop_cat AS (

    SELECT offense_id,
        CASE
            WHEN offense_parent_group='HOMICIDE OFFENSES' THEN 'violent'
            WHEN offense_parent_group='ROBBERY' THEN 'violent'
            WHEN offense='Rape' THEN 'violent'
            WHEN offense='Aggravated Assault' THEN 'violent'

            WHEN offense_parent_group='ARSON' THEN 'property'
            WHEN offense='Burglary/Breaking & Entering' THEN 'property'
            WHEN offense_parent_group='LARCENY-THEFT' THEN 'property'
            WHEN offense_parent_group='MOTOR VEHICLE THEFT' THEN 'property'
            ELSE 'other'
        END AS violent_or_property_category
    FROM {{ source('staging','seattle_crime_data_partitioned_clustered') }}
    
)
SELECT 
    -- identifiers
    report_number,
    original.offense_id as offense_id,
    -- date
    report_datetime,
    -- category
    crime_against_category,
    offense_parent_group,
    offense,
    violent_or_property_category,
    -- location
    mcpp as area,
    longitude,
    latitude
    
FROM {{ source('staging','seattle_crime_data_partitioned_clustered') }} original
INNER JOIN vio_or_prop_cat
on original.offense_id = vio_or_prop_cat.offense_id
