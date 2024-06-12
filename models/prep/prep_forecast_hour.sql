WITH forecast_hour_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *
        ,date::time AS time -- Extracting time (hours:minutes:seconds) as TIME data type
        ,TO_CHAR(date, 'HH24:MI') AS hour -- Extracting time (hours:minutes) as TEXT data type
        ,TO_CHAR(date, 'Month') AS month_of_year -- Extracting month name as text
        ,TO_CHAR(date, 'Day') AS day_of_week -- Extracting weekday name as text
    FROM forecast_hour_data
)
SELECT *
FROM add_features
