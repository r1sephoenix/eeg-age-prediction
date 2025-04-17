def get_items_params():
    return """
            SELECT 
                item,
                item_desc,
                dept,
                subdept,
                class_id,
                subclass,
                model_id
            FROM assortment_ods.items
           """


def get_receipts(checkout_items, start_date, end_date, store_id=None, city=None):
    """
    Parameters
    ----------
    checkout_items : list
                     list of checout items
    start_date : datetime.date
                 start date of the period
    end_date : datetime.date
               end date of the period
    store_id : int
               ID of the store
    city : str
           capitalised city name in russian

    Achtung!!! store_id and city can't be specified at the same time!
    """
    if store_id and city:
        print("Parameters store_id and city can't be specified at the same time!")
    if store_id:
        if isinstance(store_id, int):
            return f"""
                    SELECT *
                    FROM (
                        SELECT 
                            r.receipt_surrogate_id,  
                            r.line_item_id,
                            r.client_type,
                            r.client_loyalty_card_number as client_card,
                            r.opened_date,
                            i.model_id,
                            i.dept,
                            i.subdept,
                            i.class_id,
                            i.subclass,
                            r.store_id,
                            toYYYYMM(r.opened_date) AS month_id,
                            count(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
                            sum(r.line_quantity) AS line_quantity
                        FROM assortment_ods.dds_receipt_lines_public AS r 
                        LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
                        WHERE r.opened_date BETWEEN '{str(start_date)}' :: date AND '{str(end_date)}' :: date
                          AND r.store_id = {store_id}
                          AND line_item_id IN (
                                                SELECT item line_item_id
                                                FROM assortment_ods.items
                                                WHERE 1=1
                                                   AND item_desc NOT LIKE 'CR_%'
                                                   AND gamma IN ('A', 'Ac', 'A0', 'B')
                                                   AND line_item_id::int NOT IN ({', '.join([str(x) for x in checkout_items])})
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
                    WHERE count_items >= 2"""
        else:
            return f"""
                    SELECT *
                    FROM (
                        SELECT 
                            r.receipt_surrogate_id,  
                            r.line_item_id,
                            r.client_type,
                            r.client_loyalty_card_number as client_card,
                            r.opened_date,
                            i.model_id,
                            i.dept,
                            i.subdept,
                            i.class_id,
                            i.subclass,
                            r.store_id,
                            toYYYYMM(r.opened_date) AS month_id,
                            count(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
                            sum(r.line_quantity) AS line_quantity
                        FROM assortment_ods.dds_receipt_lines_public AS r 
                        LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
                        WHERE r.opened_date BETWEEN '{str(start_date)}' :: date AND '{str(end_date)}' :: date
                          AND r.store_id IN ({', '.join([str(s) for s in store_id])})
                          AND line_item_id IN (
                                                SELECT item line_item_id
                                                FROM assortment_ods.items
                                                WHERE 1=1
                                                   AND item_desc NOT LIKE 'CR_%'
                                                   AND gamma IN ('A', 'Ac', 'A0', 'B')
                                                   AND line_item_id::int NOT IN ({', '.join([str(x) for x in checkout_items])})
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
                    WHERE count_items >= 2"""
    if city:
        return f"""
                SELECT *
                FROM (
                    SELECT 
                        r.receipt_surrogate_id AS receipt_surrogate_id,  
                        r.line_item_id AS line_item_id,
                        r.client_type AS client_type,
                        r.client_loyalty_card_number as client_card,
                        r.opened_date,
                        r.store_id AS store_id,
                        i.model_id AS model_id,
                        i.dept AS dept,
                        i.subdept AS subdept,
                        i.class_id AS class_id,
                        i.subclass AS subclass,
                        toYYYYMM(r.opened_date) AS month_id,
                        count(DISTINCT line_item_id) OVER (PARTITION BY receipt_surrogate_id) AS count_items,
                        sum(r.line_quantity) AS line_quantity
                    FROM assortment_ods.dds_receipt_lines_public AS r 
                    LEFT JOIN assortment_ods.items AS i ON r.line_item_id = i.item 
                    LEFT JOIN assortment_ods.store AS s ON r.store_id = s.store
                    WHERE r.opened_date BETWEEN '{str(start_date)}' :: date AND '{str(end_date)}' :: date
                      AND s.district_name = '{city}'
                      AND r.line_item_id IN (
                                            SELECT item line_item_id
                                            FROM assortment_ods.items
                                            WHERE 1=1
                                               AND item_desc NOT LIKE 'CR_%'
                                               AND gamma IN ('A', 'Ac', 'A0', 'B')
                                               AND line_item_id::int NOT IN ({', '.join([str(x) for x in checkout_items])})
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
                WHERE count_items >= 2"""
        # поменял 4 на 3, т.к. группы от 3х sku рассматриваем


# def get_checkout_items(store_id=STORE, start_date=SD, end_date=ED):
#     if isinstance(store_id,int):
#         return f"""
#                 select distinct article
#                 from pactg_marts.v_pactg_ah_sale
#                 where 1=1
#                     and magasin = {store_id}
#                     and zone = 'ТЖ касс'
#                     and cal_date between '{str(start_date)}' and '{str(end_date)}'
#                 """
#     else:
#         return f"""
#                 select distinct article
#                 from pactg_marts.v_pactg_ah_sale
#                 where 1=1
#                     and magasin in ({', '.join([str(s) for s in store_id])})
#                     and zone = 'ТЖ касс'
#                     and cal_date between '{str(start_date)}' and '{str(end_date)}'
#                 """


def get_checkout_items_all():

    return f"""
            select distinct article
            from pactg_marts.v_pactg_ah_sale
            where 1=1
                and zone = 'ТЖ касс'
            """


def get_city_stores(city):
    if isinstance(city, str):
        return f"""select store
                   from assortment_ods.store
                   where store_type = 'store' and
                         district_name = '{city}'
                 """
    else:
        print("Parameter city takes str values: capitalised store name in russian!")


def get_group_receipts(stores: list, items: list, start_date, end_date):
    return f"""
    SELECT  receipt_surrogate_id,
            line_item_id item,
            store_id,
            sum(line_turnover) line_turnover,
            sum(line_margin) line_margin,
            sum(line_quantity) line_quantity
    FROM assortment_ods.dds_receipt_lines_public drlp 
    WHERE store_id IN ({str(stores)[1:-1]})
    AND line_item_id IN ({str(list(map(int, items)))[1:-1]})
    AND opened_date BETWEEN '{str(start_date)}'::date AND '{str(end_date)}'::date
    GROUP BY line_item_id, receipt_surrogate_id, store_id
    """

def get_client_receipts(stores: list, start_date, end_date):
    return f"""
    SELECT  receipt_surrogate_id,
            line_item_id item,
            store_id,
            opened_date,
            client_loyalty_card_number client_card,
            sum(line_turnover) line_turnover,
            sum(line_margin) line_margin,
            sum(line_quantity) line_quantity
    FROM assortment_ods.dds_receipt_lines_public drlp 
    WHERE store_id IN ({str(stores)[1:-1]})
    AND opened_date BETWEEN '{str(start_date)}'::date AND '{str(end_date)}'::date
    GROUP BY line_item_id, receipt_surrogate_id, store_id, opened_date, client_loyalty_card_number
    """
