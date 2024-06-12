WITH forecast_day_data AS (
    SELECT * 
    FROM staging_forecast_day
),
add_features AS (
    SELECT *,
        EXTRACT(DAY FROM date) AS day_of_month, -- day of month as a number
        TO_CHAR(date, 'Month') AS month_of_year, -- month name as a text
        EXTRACT(YEAR FROM date) AS year, -- year as a number
        TO_CHAR(date, 'Day') AS day_of_week, -- weekday name as text
        EXTRACT(WEEK FROM date) AS week_of_year, -- calendar week number as number
        CONCAT(EXTRACT(YEAR FROM date), '-', EXTRACT(WEEK FROM date)) AS year_and_week -- year-calendarweek as text like '2024-43'
    FROM forecast_day_data
)
SELECT *
FROM add_features
