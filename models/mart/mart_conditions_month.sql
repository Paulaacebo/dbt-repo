-- Common Table Expression (CTE) para combinar las tablas
WITH joining_day_location AS (
    SELECT * 
    FROM "climate"."public"."prep_forecast_day"
    LEFT JOIN "climate"."public"."staging_location"
    USING(city, region, country)
),

-- CTE para seleccionar columnas específicas
filtering_features AS (
    SELECT 
        year_and_week,
        month_of_year,
        week_of_year,
        year,
        city,
        region,
        country,
        lat,
        lon,
        timezone_id,
        max_temp_c,
        min_temp_c,
        avg_temp_c,
        total_precip_mm,
        total_snow_cm,
        avg_humidity,
        daily_will_it_rain, -- Asegúrate de que esta columna exista en la tabla
        daily_chance_of_rain, -- Asegúrate de que esta columna exista en la tabla
        daily_will_it_snow, -- Asegúrate de que esta columna exista en la tabla
        daily_chance_of_snow, -- Asegúrate de que esta columna exista en la tabla
        condition_text, -- Asegúrate de que esta columna exista en la tabla
        max_wind_kph,
        avg_vis_km,
        sunrise,
        sunset,
        moonrise,
        moonset,
        day_of_month,
        month_of_year,
        day_of_week
    FROM joining_day_location
), 

-- CTE para agregar los datos seleccionados
aggregations_adding_features AS (
    SELECT 
        year_and_week,
        month_of_year,
        week_of_year,
        year,
        city,
        region,
        country,
        lat,
        lon,
        timezone_id,
        MAX(max_temp_c) AS max_temp_c,
        MIN(min_temp_c) AS min_temp_c,
        AVG(avg_temp_c) AS avg_temp_c,
        SUM(total_precip_mm) AS total_precip_mm,
        SUM(total_snow_cm) AS total_snow_cm,
        AVG(avg_humidity) AS avg_humidity,
        SUM(daily_will_it_rain) AS will_it_rain_days, -- Verifica la existencia de esta columna
        AVG(daily_chance_of_rain) AS daily_chance_of_rain_avg, -- Verifica la existencia de esta columna
        SUM(daily_will_it_snow) AS will_it_snow_days, -- Verifica la existencia de esta columna
        AVG(daily_chance_of_snow) AS daily_chance_of_snow_avg, -- Verifica la existencia de esta columna
        SUM(CASE WHEN condition_text = 'Sunny' THEN 1 ELSE 0 END) AS sunny_days,
        SUM(CASE WHEN condition_text IN ('Overcast', 'Partly cloudy', 'Cloudy', 'Freezing fog') THEN 1 ELSE 0 END) AS other_days,
        SUM(CASE WHEN condition_text IN ('Patchy rain possible', 'Moderate or heavy rain shower', 'Light rain shower',
                                         'Mist', 'Moderate rain at times', 'Patchy light rain with thunder',
                                         'Patchy light drizzle', 'Thundery outbreaks possible', 'Heavy rain at times', 
                                         'Fog', 'Moderate or heavy rain with thunder', 'Light drizzle', 'Light rain', 
                                         'Patchy light rain', 'Heavy rain', 'Moderate rain', 'Torrential rain shower', 
                                         'Light snow showers', 'Moderate or heavy snow showers', 'Light freezing rain',
                                         'Moderate or heavy freezing rain', 'Heavy freezing drizzle') THEN 1 ELSE 0 END) AS rainy_days,
        SUM(CASE WHEN condition_text IN ('Patchy light snow', 'Heavy snow', 'Light sleet', 'Light snow', 
                                         'Moderate snow', 'Light sleet showers', 'Patchy heavy snow',
                                         'Patchy moderate snow', 'Moderate or heavy snow with thunder',
                                         'Moderate or heavy sleet', 'Blizzard', 'Blowing snow', 'Patchy snow posible', 
                                         'Moderate or heavy showers of ice pellets', 'Patchy light snow with thunder',
                                         'Patchy sleet posible', 'Ice pellets') THEN 1 ELSE 0 END) AS snowy_days
    FROM filtering_features
    GROUP BY year_and_week, month_of_year, week_of_year, year, city, region, country, lat, lon, timezone_id
    ORDER BY city
)

-- Selección final de la CTE agregada
SELECT * 
FROM aggregations_adding_features;
