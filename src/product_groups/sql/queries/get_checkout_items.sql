-- Назначение: Получить уникальные товары из зоны "ТЖ касс" за два периода (текущий и прошлогодний)
-- Аргументы: start_date_current, end_date_current, start_date_lastyear, end_date_lastyear, store_id

SELECT DISTINCT article AS item,
       cal_date
FROM pactg_marts.v_pactg_ah_sale
WHERE 1=1
  AND magasin = {{ store_id }}
  AND zone = 'ТЖ касс'
  AND (
      cal_date BETWEEN '{{ start_date_current }}' AND '{{ end_date_current }}'
      OR cal_date BETWEEN '{{ start_date_lastyear }}' AND '{{ end_date_lastyear }}'
  );
