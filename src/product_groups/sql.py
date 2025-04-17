import config


# ----------------------------------------------------------------------------------
#               ITEMS DATA
# ----------------------------------------------------------------------------------
def _get_items_data():
    return '''
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
FROM assortment_ods.items
'''


def _get_checkout_items(start_date_current=config.SD_CUR, end_date_current=config.ED_CUR, 
                        start_date_lastyear=config.SD_LY, end_date_lastyear=config.ED_LY, 
                        store_id=config.STORE_ID):
    return f"""
select distinct article as item,
       cal_date
from pactg_marts.v_pactg_ah_sale
where 1=1
	and magasin = {str(store_id)}
	and zone = 'ТЖ касс'
	and (cal_date between '{str(start_date_current)}' and '{str(end_date_current)}'
    or cal_date between '{str(start_date_lastyear)}' and '{str(end_date_lastyear)}')
"""


# ----------------------------------------------------------------------------------
#               RECEIPTS DATA
# ----------------------------------------------------------------------------------
def _get_receipt_data(checkout_items_list,
                      start_date_current=config.SD_CUR, end_date_current=config.ED_CUR, 
                      start_date_lastyear=config.SD_LY, end_date_lastyear=config.ED_LY, 
                      store_id=config.STORE_ID):
    return f"""
SELECT *
FROM (
    SELECT 
        r.receipt_surrogate_id, 
        r.client_type,
        r.line_item_id,
        i.model_id,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        r.store_id,
        --toYYYYMM(r.opened_date) AS month_id,
        r.opened_date,
        count(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
        sum(r.line_quantity) AS line_quantity
    FROM assortment_ods.dds_receipt_lines_public AS r 
    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
    WHERE (r.opened_date BETWEEN '{str(start_date_lastyear)}' AND '{str(end_date_lastyear)}' 
           OR r.opened_date BETWEEN '{str(start_date_current)}' AND '{str(end_date_current)}')
      AND store_id = {str(store_id)}
      AND line_item_id IN (
                            SELECT item line_item_id
                            FROM assortment_ods.items
                            WHERE 1=1
                               AND item_desc NOT LIKE 'CR_%'
                               AND gamma IN ('A', 'Ac', 'A0', 'B')
                               AND line_item_id::int NOT IN ({', '.join([str(x) for x in checkout_items_list])})
                               AND subdept <> 940
       )
    GROUP BY 
        r.receipt_surrogate_id, 
        r.client_type,
        r.line_item_id,
        r.store_id,
        i.model_id,
        i.dept,
        i.subdept,
        i.class_id,
        i.subclass,
        --toYYYYMM(r.opened_date)
        r.opened_date
        )
WHERE count_items >= 3"""


def _get_receipts_with_client_id(checkout_items_list,
                                 start_date_current=config.SD_CUR, end_date_current=config.ED_CUR, 
                                 start_date_lastyear=config.SD_LY, end_date_lastyear=config.ED_LY, 
                                 store_id=config.STORE_ID):
    return f'''
SELECT *
FROM (
    SELECT 
        r.client_loyalty_card_number client_card,
        client_type,
        r.receipt_surrogate_id receipt_surrogate_id,  
        r.line_item_id line_item_id,
        r.store_id store_id,
        sum(r.line_turnover) AS line_turnover,
        r.opened_date as opened_date,
        toYYYYMMDD(r.opened_date) AS day_id,
        count(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
        sum(r.line_quantity) AS line_quantity
    FROM assortment_ods.dds_receipt_lines_public AS r 
    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
    WHERE (r.opened_date BETWEEN '{str(start_date_lastyear)}' AND '{str(end_date_lastyear)}' 
           OR r.opened_date BETWEEN '{str(start_date_current)}' AND '{str(end_date_current)}')
      AND store_id = {str(store_id)}
      AND line_item_id IN (
                            SELECT item line_item_id
                            FROM assortment_ods.items
                            WHERE 1=1
                               AND item_desc NOT LIKE 'CR_%'
                               AND gamma IN ('A', 'Ac', 'A0', 'B')
                               AND line_item_id::int NOT IN ({', '.join([str(x) for x in checkout_items_list])})
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
WHERE count_items >= 3
'''