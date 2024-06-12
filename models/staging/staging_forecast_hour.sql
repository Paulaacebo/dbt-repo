-- First Step: Extract hourly weather data
WITH forecast_hour_raw AS (
    SELECT
        (extracted_data -> 'location' ->> 'name')::VARCHAR(255) AS city,
        (extracted_data -> 'location' ->> 'region')::VARCHAR(255) AS region,
        (extracted_data -> 'location' ->> 'country')::VARCHAR(255) AS country,
        (extracted_data -> 'forecast' -> 'forecastday' -> 0 ->> 'date')::DATE AS date,
        json_array_elements(extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'hour')::json AS hour_data
    FROM {{source("staging", "weather_raw")}}
),
-- Second Step: Extract detailed hourly weather information
forecast_hour_data AS (
    SELECT
        city,
        region,
        country,
        date,
        (hour_data ->> 'time')::TIMESTAMP AS time,
        (hour_data ->> 'temp_c')::NUMERIC AS temp_c,
        (hour_data ->> 'is_day')::INT AS is_day,
        (hour_data -> 'condition' ->> 'text')::VARCHAR(255) AS condition_text,
        (hour_data -> 'condition' ->> 'icon')::VARCHAR(255) AS condition_icon,
        (hour_data -> 'condition' ->> 'code')::VARCHAR(255) AS condition_code,
        (hour_data ->> 'wind_kph')::NUMERIC AS wind_kph,
        (hour_data ->> 'wind_degree')::NUMERIC AS wind_degree,
        (hour_data ->> 'wind_dir')::VARCHAR(255) AS wind_dir,
        (hour_data ->> 'pressure_mb')::NUMERIC AS pressure_mb,
        (hour_data ->> 'precip_mm')::NUMERIC AS precip_mm,
        (hour_data ->> 'humidity')::NUMERIC AS humidity,
        (hour_data ->> 'cloud')::NUMERIC AS cloud,
        (hour_data ->> 'feelslike_c')::NUMERIC AS feelslike_c,
        (hour_data ->> 'windchill_c')::NUMERIC AS windchill_c,
        (hour_data ->> 'heatindex_c')::NUMERIC AS heatindex_c,
        (hour_data ->> 'dewpoint_c')::NUMERIC AS dewpoint_c,
        (hour_data ->> 'will_it_rain')::INT AS will_it_rain,
        (hour_data ->> 'chance_of_rain')::NUMERIC AS chance_of_rain,
        (hour_data ->> 'will_it_snow')::INT AS will_it_snow,
        (hour_data ->> 'chance_of_snow')::NUMERIC AS chance_of_snow,
        (hour_data ->> 'vis_km')::NUMERIC AS vis_km,
        (hour_data ->> 'gust_kph')::NUMERIC AS gust_kph,
        (hour_data ->> 'uv')::NUMERIC AS uv
    FROM forecast_hour_raw
)
SELECT *
FROM forecast_hour_data

