
-- Назначение: Выгрузить чеки с деталями по клиентам, товарам, total_turnover и общим оборотом по магазину
-- Аргументы: start_date, end_date, store_id (опц.), city (опц.), checkout_items

WITH item_turnover AS (
    SELECT
        line_item_id,
        SUM(line_turnover) AS total_turnover
    FROM assortment_ods.dds_receipt_lines_public
    WHERE opened_date BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date
      AND line_item_id::int NOT IN ({{ checkout_items | join(", ") }})
    GROUP BY line_item_id
),
store_total_turnover AS (
    SELECT
        store_id,
        SUM(line_turnover) AS store_turnover
    FROM assortment_ods.dds_receipt_lines_public
    WHERE opened_date BETWEEN '{{ start_date }}'::date AND '{{ end_date }}'::date
      AND line_item_id::int NOT IN ({{ checkout_items | join(", ") }})
    GROUP BY store_id
)

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
        m.model_name_ru,
        m.model_name_en,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        toYYYYMM(r.opened_date) AS month_id,
        COUNT(DISTINCT r.line_item_id) OVER (PARTITION BY r.receipt_surrogate_id) AS count_items,
        SUM(r.line_quantity) AS line_quantity,
        SUM(r.line_turnover) AS line_turnover,
        it.total_turnover,
        st.store_turnover
    FROM assortment_ods.dds_receipt_lines_public AS r
    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item
    LEFT JOIN test.models AS m ON i.model_id = m.model_id
    LEFT JOIN item_turnover AS it ON r.line_item_id = it.line_item_id
    LEFT JOIN store_total_turnover AS st ON r.store_id = st.store_id
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
        m.model_name_ru,
        m.model_name_en,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        toYYYYMM(r.opened_date),
        it.total_turnover,
        st.store_turnover
)
WHERE count_items >= 3;
