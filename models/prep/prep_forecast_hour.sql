WITH forecast_hour_data AS (
    SELECT * 
    FROM {{ref('staging_forecast_hour')}}
),
add_features AS (
    SELECT *
        ,CAST(date AS TIMESTAMP) AS timestamp_with_default_time -- Convert date to timestamp (default time is 00:00:00)
        ,EXTRACT(HOUR FROM CAST(date AS TIMESTAMP))::TEXT || ':' || LPAD(EXTRACT(MINUTE FROM CAST(date AS TIMESTAMP))::TEXT, 2, '0') AS hour -- Extracting time as TEXT data type
        ,TO_CHAR(date, 'Month') AS month_of_year -- Extracting month name as text
        ,TO_CHAR(date, 'Day') AS day_of_week -- Extracting weekday name as text
    FROM forecast_hour_data
)
SELECT *
FROM add_features;

