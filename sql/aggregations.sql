CREATE OR REPLACE VIEW daily_weather AS
SELECT city, DATE(obs_time) AS day,
       ROUND(AVG(temp_c),1) AS avg_temp_c, ROUND(MAX(temp_c),1) AS max_temp_c,
       ROUND(MIN(temp_c),1) AS min_temp_c, ROUND(SUM(rain_mm),1) AS total_rain_mm
FROM weather_hourly GROUP BY city, DATE(obs_time);

CREATE OR REPLACE VIEW monthly_weather AS
SELECT city, DATE_FORMAT(obs_time, '%Y-%m') AS month,
       ROUND(AVG(temp_c),1) AS avg_temp_c, ROUND(SUM(rain_mm),1) AS total_rain_mm,
       COUNT(DISTINCT DATE(obs_time)) AS days_recorded
FROM weather_hourly GROUP BY city, DATE_FORMAT(obs_time, '%Y-%m');
