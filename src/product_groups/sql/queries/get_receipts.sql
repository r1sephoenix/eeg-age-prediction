-- Назначение: Получить чеки за период с фильтрацией по store_id или городу
-- Аргументы: start_date, end_date, store_id (опц.), city (опц.), checkout_items
-- Внимательно!: store_id и city не могут быть заданы одновременно!

SELECT *
FROM (
    SELECT 
        r.receipt_surrogate_id,  
        r.line_item_id,
        r.client_type,
        r.client_loyalty_card_number AS client_card,
        r.opened_date,
        r.store_id,
        i.model_id,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        toYYYYMM(r.opened_date) AS month_id,
        COUNT(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
        SUM(r.line_quantity) AS line_quantity
    FROM assortment_ods.dds_receipt_lines_public AS r 
    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
    {% if city %}
    LEFT JOIN assortment_ods.store AS s ON r.store_id = s.store
    {% endif %}
    WHERE r.opened_date BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date
    {% if store_id %}
      AND r.store_id {% if store_id is iterable and store_id is not string %}IN ({{ store_id | join(", ") }}){% else %}= {{ store_id }}{% endif %}
    {% endif %}
    {% if city %}
      AND s.district_name = '{{ city }}'
    {% endif %}
    AND r.line_item_id IN (
        SELECT item AS line_item_id
        FROM assortment_ods.items
        WHERE 1=1
            AND item_desc NOT LIKE 'CR_%'
            AND gamma IN ('A', 'Ac', 'A0', 'B')
            AND line_item_id::int NOT IN ({{ checkout_items | join(", ") }})
            AND subdept <> 940
    )
    GROUP BY 
        r.receipt_surrogate_id,  
        r.line_item_id,
        r.client_type,
        r.client_loyalty_card_number,
        r.opened_date,
        r.store_id,
        i.model_id,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        toYYYYMM(r.opened_date)
)
WHERE count_items >= 2;
