-- Назначение: Получить полные характеристики товаров

SELECT 
    item,
    item_desc,
    dept,
    dept_name,
    subdept,
    subdept_name,
    class_id, 
    class_name,
    subclass,
    subclass_name,
    model_id
FROM assortment_ods.items;
