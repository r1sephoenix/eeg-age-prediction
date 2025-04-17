-- Назначение: Получить чеки с client_id, фильтрация по периоду и товарам
-- Аргументы: start_date_current, end_date_current, start_date_lastyear, end_date_lastyear, store_id, checkout_items_list

SELECT *
FROM (
    SELECT 
        r.client_loyalty_card_number AS client_card,
        client_type,
        r.receipt_surrogate_id AS receipt_surrogate_id,  
        r.line_item_id AS line_item_id,
        r.store_id AS store_id,
        SUM(r.line_turnover) AS line_turnover,
        r.opened_date AS opened_date,
        toYYYYMMDD(r.opened_date) AS day_id,
        COUNT(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
        SUM(r.line_quantity) AS line_quantity
    FROM assortment_ods.dds_receipt_lines_public AS r 
    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
    WHERE (
        r.opened_date BETWEEN '{{ start_date_lastyear }}' AND '{{ end_date_lastyear }}'
        OR r.opened_date BETWEEN '{{ start_date_current }}' AND '{{ end_date_current }}'
    )
      AND store_id = {{ store_id }}
      AND line_item_id IN (
            SELECT item AS line_item_id
            FROM assortment_ods.items
            WHERE 1=1
              AND item_desc NOT LIKE 'CR_%'
              AND gamma IN ('A', 'Ac', 'A0', 'B')
              AND line_item_id::int NOT IN ({{ checkout_items_list | join(", ") }})
              AND subdept <> 940
       )
    GROUP BY 
        r.client_loyalty_card_number,
        client_type,
        r.receipt_surrogate_id,  
        r.line_item_id,
        r.store_id,
        opened_date,
        toYYYYMMDD(r.opened_date)
)
WHERE count_items >= 3;
