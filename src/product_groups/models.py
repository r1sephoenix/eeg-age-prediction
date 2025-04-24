import pandas as pd
import numpy as np
import time
import datetime as dt
from collections import Counter
from mlxtend.frequent_patterns import fpgrowth, association_rules
from mlxtend.preprocessing import TransactionEncoder

def create_groups_with_anchors(df_receipt, df_items, min_sales=40, support_type='quantile', store_id='2', log=False):
    client_type = df_receipt.client_type.unique()[0]
    project_type = df_receipt.project_type.unique()[0]

    start_time = time.time()

    # Группируем и фильтруем сразу
    df_list_models = df_receipt.groupby('receipt_surrogate_id')['comb_id'].unique().reset_index(name='list_stockcode')
    df_list_models['list_stockcode'] = df_list_models['list_stockcode'].apply(lambda x: list(set(x)))
    df_list_models['n_comb_id'] = df_list_models['list_stockcode'].apply(len)
    df_list_models = df_list_models[df_list_models['n_comb_id'] > 1]

    if support_type == 'quantile':
        all_items = np.concatenate(df_list_models['list_stockcode'].values)
        item_counts = pd.Series(all_items).value_counts(normalize=True)
        min_support = item_counts.quantile(0.8)
    else:
        min_support = round(min_sales / df_list_models.receipt_surrogate_id.nunique(), 6)

    min_support = 0.01
    dataset = df_list_models['list_stockcode'].values
    te = TransactionEncoder()
    te_ary = te.fit(dataset).transform(dataset)
    df = pd.DataFrame(te_ary, columns=te.columns_)

    frequent_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True)
    arl_df = association_rules(frequent_itemsets, metric="lift", min_threshold=1.2).sort_values('lift', ascending=False)

    arl_df['group'] = arl_df.apply(lambda row: frozenset(row['antecedents']).union(row['consequents']), axis=1)
    arl_df['group_str'] = arl_df['group'].apply(lambda x: str(sorted(x)))
    arl_df = arl_df.drop_duplicates(subset='group_str').reset_index(drop=True)
    arl_df['group_id'] = np.arange(1, arl_df.shape[0] + 1)

    # Кэширование comb_id → department
    comb_to_dep = df_items.set_index('comb_id')['dept'].to_dict()
    def fast_main_dep(group):
        deps = [comb_to_dep.get(i) for i in group if comb_to_dep.get(i) is not None]
        return pd.Series(deps).mode()[0] if deps else None

    arl_df['main_dep'] = arl_df['group'].apply(fast_main_dep)

    # Подсчёт уникальных товаров
    comb_to_item = df_items.set_index('comb_id')['item'].to_dict()
    def count_unique_items(group):
        return len(set(comb_to_item.get(i) for i in group if i in comb_to_item))

    arl_df['n_items'] = arl_df['group'].apply(count_unique_items)

    antecedent_counts = Counter(item for items in arl_df['antecedents'] for item in items)
    consequent_counts = Counter(item for items in arl_df['consequents'] for item in items)

    def identify_anchor_items(group):
        return [item for item in group if antecedent_counts[item] > consequent_counts.get(item, 0)]

    arl_df['anchor_items'] = arl_df['group'].apply(identify_anchor_items)

    df_group = arl_df[['group', 'group_id', 'main_dep', 'n_items', 'support', 'confidence', 'lift', 'leverage',
                       'conviction', 'anchor_items']].copy()
    df_group['len_g'] = df_group['group'].apply(len)
    df_group['n_anchors'] = df_group['anchor_items'].apply(len)
    df_group['client_type'] = client_type

    stop_time = time.time()
    total_exec_time = round(stop_time - start_time, 1)
    group_month = (dt.date.today() + dt.timedelta(1)).strftime('%h %y')

    log_data = [store_id, client_type, project_type,
                df_receipt['receipt_surrogate_id'].nunique(), df_receipt['comb_id'].nunique(),
                min_support, min_sales,
                df_group.shape[0], df_group['len_g'].max(),
                total_exec_time, group_month]

    if log:
        return df_group, log_data
    else:
        return df_group
