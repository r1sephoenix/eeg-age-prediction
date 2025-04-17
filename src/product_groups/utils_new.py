import pandas as pd
import numpy as np
from tqdm import tqdm
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
import config
import time
import datetime as dt
import pyarrow.parquet as pq

from bayes_opt import BayesianOptimization
from clickhouse_driver import Client
import matplotlib.pyplot as plt

import re


def get_data_ch(sql: str, connection: str, user: str, password: str) -> pd.DataFrame:
    client = Client(host=connection, user=user, password=password)
    data, columns = client.execute(sql, columnar=True, with_column_types=True)
    df = pd.DataFrame({re.sub(r"\W", "_", col[0]): d for d, col in zip(data, columns)})
    return df


# ----------------------------------------------------------------------------------
#               PREP EXPERT PROJECTS
# ----------------------------------------------------------------------------------
def group_formula(df):
    if int(df.columns.max()[-1:]) == 4:
        # nomenc list
        df['items'] = df['item_1'].astype(str) + ',' + \
                      df['item_2'].astype(str) + ',' + \
                      df['item_3'].astype(str) + ',' + \
                      df['item_4'].astype(str)
        df['items'] = [d.split(',') for d in df['items'].values]
        df['items'] = [[i for i in d if i != 'nan'] for d in df['items'].values]

        # numbers
        df['n_min'] = df['n_min_1'].astype(str) + ',' + \
                      df['n_min_2'].astype(str) + ',' + \
                      df['n_min_3'].astype(str) + ',' + \
                      df['n_min_4'].astype(str)
        df['n_min'] = [d.split(',') for d in df['n_min'].values]
        df['n_min'] = [[i for i in d if i != 'nan'] for d in df['n_min'].values]

        # units
        df['unit'] = df['unit_1'].astype(str) + ',' + \
                     df['unit_2'].astype(str) + ',' + \
                     df['unit_3'].astype(str) + ',' + \
                     df['unit_4'].astype(str)
        df['unit'] = [d.split(',') for d in df['unit'].values]
        df['unit'] = [[i for i in d if i != 'nan'] for d in df['unit'].values]
    else:
        # nomenc list
        df['items'] = df['item_1'].astype(str) + ',' + \
                      df['item_2'].astype(str) + ',' + \
                      df['item_3'].astype(str) + ',' + \
                      df['item_4'].astype(str) + ',' + \
                      df['item_5'].astype(str) + ',' + \
                      df['item_6'].astype(str) + ',' + \
                      df['item_7'].astype(str)
        df['items'] = [d.split(',') for d in df['items'].values]
        df['items'] = [[i for i in d if i != 'nan'] for d in df['items'].values]

        # numbers
        df['n_min'] = df['n_min_1'].astype(str) + ',' + \
                      df['n_min_2'].astype(str) + ',' + \
                      df['n_min_3'].astype(str) + ',' + \
                      df['n_min_4'].astype(str) + ',' + \
                      df['n_min_5'].astype(str) + ',' + \
                      df['n_min_6'].astype(str) + ',' + \
                      df['n_min_7'].astype(str)
        df['n_min'] = [d.split(',') for d in df['n_min'].values]
        df['n_min'] = [[i for i in d if i != 'nan'] for d in df['n_min'].values]

        # units
        df['unit'] = df['unit_1'].astype(str) + ',' + \
                     df['unit_2'].astype(str) + ',' + \
                     df['unit_3'].astype(str) + ',' + \
                     df['unit_4'].astype(str) + ',' + \
                     df['unit_5'].astype(str) + ',' + \
                     df['unit_6'].astype(str) + ',' + \
                     df['unit_7'].astype(str)
        df['unit'] = [d.split(',') for d in df['unit'].values]
        df['unit'] = [[i for i in d if i != 'nan'] for d in df['unit'].values]
    return df


def get_project_df(path, project_cols=config.PROJECT_COLS, drop_service=False, drop_item_cols=False):
    if "ИС" in path:
        project_df = pd.read_excel(path).iloc[2:]
        project_df[project_df.columns[1]] = project_df[project_df.columns[1]].astype(str) + '-' + \
                                            project_df[project_df.columns[2]].astype(str)
        project_df = project_df.drop(project_df.columns[2], axis=1)
        project_df = project_df.rename(columns={k: v for k, v in zip(project_df.columns,
                                                                     project_cols)}).reset_index(drop=True)
    elif "Кухни" in path:
        project_df = pd.read_excel(path).iloc[1:]
        n_items_in_pj = sum([1 if 'товар' in c else 0 for c in list(project_df.iloc[:, 3:].columns)])
        additional_cols = [[f'item_{i}',
                            f'n_min_{i}',
                            f'unit_{i}'] for i in np.arange(int(project_cols[-1:][0][-1:]) + 1, n_items_in_pj + 1)]
        additional_cols = [col for col_set in additional_cols for col in col_set]
        kitchen_cols = project_cols[:3] + project_cols[4:] + additional_cols
        project_df = project_df.rename(columns={k: v for k, v in zip(project_df.columns,
                                                                     kitchen_cols)}).reset_index(drop=True)
    else:
        project_df = pd.read_excel(path).iloc[2:, : 16]
        project_df = project_df.rename(columns={k: v for k, v in zip(project_df.columns,
                                                                     project_cols)}).reset_index(drop=True)
    if drop_service:
        project_df = project_df[-project_df.formula.str.contains('услуга')].reset_index(drop=True)
    project_df = group_formula(project_df)

    if drop_item_cols:
        project_df = project_df.drop(project_df.filter(like='item_').columns, axis=1)
        project_df = project_df.drop(project_df.filter(like='n_min_').columns, axis=1)
        project_df = project_df.drop(project_df.filter(like='unit_').columns, axis=1)
        return project_df
    else:
        return project_df


def get_full_project_df(projects_paths=config.PROJECT_PATHS):
    project_df = pd.DataFrame()
    for path in projects_paths:
        project_df = project_df.append(get_project_df(path,
                                                      drop_item_cols=True
                                                      ),
                                       ignore_index=True)
    project_df = project_df.drop('formula', axis=1)
    # project_df = project_df[-project_df.n_min_1.isnull()].reset_index(drop=True)
    project_df['project_id'] = np.arange(1, project_df.shape[0] + 1)
    return project_df


def get_combid_from_itemset(item_set, df_items):
    item_set = item_set.replace('.', '_')
    return df_items[df_items.comb_id.str.contains(item_set)].comb_id.unique()


def get_small_project_df(project_df, df_items):
    pj_df = pd.DataFrame()
    for i in tqdm(project_df.index):
        item_set = project_df.iloc[i]['items']
        item_set = [i_s for i_s in item_set if 'Услуга' not in i_s]
        project_id = project_df.iloc[i]['project_id']
        qs = project_df.iloc[i]['n_min']
        units = project_df.iloc[i]['unit']
        if len(qs) == 0 or len(units) == 0:
            continue

        comb_id_list = [get_combid_from_itemset(i_s, df_items) for i_s in item_set]
        #         assert len(comb_id_list) + len(item_set)
        pj_df = pj_df.append(pd.DataFrame({'project_id': [project_id] * len(item_set),
                                           'item_set': item_set,
                                           'comb_id_set': comb_id_list,
                                           'qs': qs,
                                           'units': units[:len(qs)]}),
                             ignore_index=True)
    pj_df['item_set'] = [i_s.replace('.', '_') for i_s in pj_df['item_set'].values]
    pj_df['len_project'] = pj_df.groupby('project_id').item_set.transform('count')
    return pj_df


def load_project_df(fname=config.PROJECT_DF_PATH):
    project_df = pq.read_table(fname)
    project_df = project_df.to_pandas()
    return project_df


# ----------------------------------------------------------------------------------
#               PREP ITEMS & RECEIPTS DATS
# ----------------------------------------------------------------------------------

def create_comb_id_col(df):
    """
    DF must contain columns model_id, dept, subdept, class_id, subclass
    """
    df['comb_id'] = df.model_id.astype(str) + '_' + \
                    df.dept.astype(str) + '_' + \
                    df.subdept.astype(str) + '_' + \
                    df.class_id.astype(str) + '_' + \
                    df.subclass.astype(str)
    return df


def get_model_df(model_data_path=config.MODELS_DATA_PATH, verbose=False):
    data_models = pd.read_excel(model_data_path, header=0)
    data_models = data_models[['Model ID ATT_99311', 'Наименование модели (RU) ATT_98933']]
    data_models.columns = ['model_modelID', 'model_desc']
    if verbose:
        data_models.info()
        print('N dup models:', data_models.model_modelID.duplicated().sum())

    data_models = data_models.drop(2454).reset_index(drop=True)

    data_models.dropna(inplace=True)
    for i in range(len(data_models)):
        data_models['model_modelID'].iloc[i] = data_models['model_modelID'].iloc[i][4:]
    data_models['model_modelID'] = data_models['model_modelID'].astype('int')
    if verbose:
        print('Models DF shape:', data_models.shape)
        print('N models:', data_models.model_modelID.nunique())
    return data_models


def create_model_name_col(df_items, df_models):
    df_items = df_items.merge(df_models.rename(columns={'model_modelID': 'model_id',
                                                        'model_desc': 'model_name'}),
                              on='model_id',
                              how='left',
                              validate='m:1')
    df_items['model_name'] = df_items.model_name.fillna(df_items.dept_name.str.lower() + ' ' + \
                                                        df_items.subdept_name.str.lower() + ' ' + \
                                                        df_items.class_name.str.lower() + ' ' + \
                                                        df_items.subclass_name.str.lower())
    return df_items


def get_checkout_items_list(checkout_items_df):
    checkout_items_list = checkout_items_df.item.unique()
    return checkout_items_list


def get_checkout_items_pq(checkout_items_df, store_id=config.STORE_ID):
    checkout_items_df = checkout_items_df[
        (checkout_items_df.store_id == store_id) &
        (((checkout_items_df.cal_date > config.SD_LY) & (checkout_items_df.cal_date < config.ED_LY)) |
         ((checkout_items_df.cal_date > config.SD_CUR) & (checkout_items_df.cal_date < config.ED_CUR)))
        ].copy()
    checkout_items_list = checkout_items_df.item.unique()
    return checkout_items_list


def clear_receipts(df_items, checkout_items_list, df_receipt, verbose=False):
    flower_items = df_items[(df_items.dept == 9) & (df_items.subdept == 940)].item.unique()
    removed_items_mask = (df_receipt.line_item_id.isin(flower_items)) | \
                         (df_receipt.line_item_id.isin(checkout_items_list))
    if verbose:
        print('N Flower items:', len(flower_items))
        print('N checkout items:', len(checkout_items_list))

    assert df_receipt[removed_items_mask].shape[0] == 0, "Checkout items and/or 940 subdept isin receipts!"
    print('Receipts are clear!')


def split_by_client_type(df_receipt):
    df_receipt_b2b = df_receipt[df_receipt.client_type == 'B2B'].copy()
    df_receipt_b2c = df_receipt[df_receipt.client_type == 'B2C'].copy()
    assert df_receipt_b2b.shape[0] + df_receipt_b2c.shape[0] == df_receipt.shape[0]

    print('DF receipt B2B shape:', df_receipt_b2b.shape)
    print('N B2B receipts:', df_receipt_b2b.receipt_surrogate_id.nunique())
    print('DF receipt B2C shape:', df_receipt_b2c.shape)
    print('N B2C receipts:', df_receipt_b2c.receipt_surrogate_id.nunique())
    return df_receipt_b2b, df_receipt_b2c


def group_receipts(df_receipt, n_days=7, short=True):
    # Step 1: Prepare initial DataFrame
    df_receipt_days = df_receipt[['client_card', 'receipt_surrogate_id', 'opened_date']].drop_duplicates()
    df_receipt_days = df_receipt_days.sort_values(by=["client_card", "opened_date"]).reset_index(drop=True)

    # Step 2: Calculate the number of receipts per client
    df_receipt_days['receipts_on_client'] = df_receipt_days.groupby('client_card')['receipt_surrogate_id'] \
        .transform('count')
    df_receipt_days = df_receipt_days[df_receipt_days.receipts_on_client > 1].dropna()

    # Step 3: Calculate days after previous and days till next receipt
    df_receipt_days["days_after_prev"] = df_receipt_days.groupby('client_card')['opened_date'] \
        .apply(lambda x: x.diff().dt.days.fillna(0)).astype(int)
    df_receipt_days["days_till_next"] = df_receipt_days.groupby('client_card')['opened_date'] \
        .apply(lambda x: x.diff(-1).dt.days.fillna(0)).shift(1).astype(int)

    # Step 4: Handle first and last receipt for each client
    first_receipts = df_receipt_days.drop_duplicates(subset="client_card", keep="first").index
    last_receipts = df_receipt_days.drop_duplicates(subset="client_card", keep="last").index
    df_receipt_days.loc[first_receipts, 'days_after_prev'] = 0
    df_receipt_days.loc[last_receipts, 'days_till_next'] = 0

    # Step 5: Identify same-date sales
    zero_days_mask = (df_receipt_days.days_after_prev == 0) | (df_receipt_days.days_till_next == 0)
    same_date_sales = df_receipt_days[zero_days_mask] \
        .groupby(['client_card', 'opened_date'])['receipt_surrogate_id'] \
        .count().reset_index(name='n_sales')
    same_date_sales = same_date_sales[same_date_sales.n_sales > 1]

    # Step 6: Merge same-day sales back into the DataFrame
    df_receipt_sameday = df_receipt_days.merge(same_date_sales[['client_card', 'opened_date']],
                                               on=['client_card', 'opened_date'], how='left')

    # Step 7: Apply filter for days within the window
    days_mask = (
            ((df_receipt_days.days_after_prev == 0) & (df_receipt_days.days_till_next <= n_days)) |
            ((df_receipt_days.days_after_prev != 0) &
             (df_receipt_days.days_after_prev <= n_days) &
             (df_receipt_days.days_till_next != 0)) |
            ((df_receipt_days.days_after_prev != 0) &
             (df_receipt_days.days_till_next != 0) &
             (df_receipt_days.days_till_next <= n_days)) |
            ((df_receipt_days.days_after_prev <= n_days) & (df_receipt_days.days_till_next == 0))
    )
    df_receipt_days_filtered = df_receipt_days[days_mask].copy()

    # Step 8: Combine data and remove duplicates
    df_receipt_res = pd.concat([df_receipt_days_filtered, df_receipt_sameday], ignore_index=True) \
        .drop_duplicates(subset=['client_card', 'receipt_surrogate_id', 'opened_date']) \
        .sort_values(['client_card', 'opened_date']).reset_index(drop=True)

    # Step 9: Assign a group ID to the receipts
    df_receipt_res['days'] = df_receipt_res.apply(
        lambda row: min(row['days_after_prev'], row['days_till_next'], n_days), axis=1)

    # Step 10: Create group IDs for consecutive receipts within the window
    index_groupid_dict = {}
    id_rec = 1
    for i in tqdm(range(1, len(df_receipt_res)), desc='Grouping receipts'):
        if df_receipt_res.iloc[i - 1].client_card != df_receipt_res.iloc[i].client_card:
            id_rec += 1
        if df_receipt_res.iloc[i].days <= n_days and df_receipt_res.iloc[i].days_till_next <= n_days:
            index_groupid_dict[df_receipt_res.iloc[i].name] = id_rec
        elif df_receipt_res.iloc[i].days_till_next > n_days:
            index_groupid_dict[df_receipt_res.iloc[i].name] = id_rec
            id_rec += 1

    df_receipt_res['rec_group_id'] = df_receipt_res.index.map(index_groupid_dict)

    if short:
        return df_receipt_res[['client_card', 'receipt_surrogate_id', 'opened_date', 'rec_group_id']]
    else:
        return df_receipt_res

def merge_receipts(df_receipt_ct, df_receipt_groupped_ct):
    """
    Returns merged regular and grouped receipts of one client type
    """
    df_receipt_ct = df_receipt_ct.merge(df_receipt_groupped_ct[['receipt_surrogate_id', 'opened_date', 'rec_group_id']],
                                        on=['receipt_surrogate_id', 'opened_date'],
                                        how='left',
                                        validate='m:m')
    print('DF receipt shape:', df_receipt_ct.shape)
    print('N groupped receipts:', df_receipt_ct.rec_group_id.nunique())

    df_receipt_ct['rec_group_id'] = df_receipt_ct['rec_group_id'].fillna(df_receipt_ct['receipt_surrogate_id']).astype(
        int)
    df_receipt_ct['receipt_surrogate_id'] = df_receipt_ct['rec_group_id']
    del df_receipt_ct['rec_group_id']
    return df_receipt_ct


def get_receipt_project_items_df(df_receipt_ct, project_items_list):
    """
    Returns Pandas DF with receipts and lists of their items if project items were among these items
    """
    df_receipt_ct_listed = df_receipt_ct[df_receipt_ct.comb_id.isin(project_items_list)].groupby(
        'receipt_surrogate_id').comb_id.unique().reset_index()
    df_receipt_ct_listed['comb_id'] = [str(sorted(cid)) for cid in df_receipt_ct_listed.comb_id.values]
    return df_receipt_ct_listed


def get_receipts_with_projects_list(small_project_df, df_receipt_ct_listed):
    receipts_dict = {}
    for pid in tqdm(small_project_df.project_id.unique(), desc="Searching for projects"):
        receipt_list = []
        tmp_item_sets = small_project_df[small_project_df.project_id == pid].item_set.values
        for item in tmp_item_sets:
            tmp_list = set(df_receipt_ct_listed[
                               df_receipt_ct_listed.comb_id.str.contains(item)].receipt_surrogate_id.unique())
            if len(tmp_list) > 0:
                receipt_list.append(tmp_list)
            else:
                receipt_list = []
                break
        if len(receipt_list) == 0:
            receipts_dict[pid] = set()
        else:
            receipts_dict[pid] = set(receipt_list[0]).intersection(*receipt_list[1:])

    project_receipt_df = pd.DataFrame({'project_id': receipts_dict.keys(),
                                       'receipts': receipts_dict.values()})
    project_receipt_df['n_receipts'] = [len(rec) for rec in project_receipt_df['receipts'].values]
    project_receipt_df = project_receipt_df.merge(small_project_df[['project_id', 'len_project']].drop_duplicates(),
                                                  on='project_id',
                                                  how='left',
                                                  validate='1:1')
    project_receipt_df = project_receipt_df[project_receipt_df['n_receipts'] > 0]
    receipts_with_projects_list = set().union(*project_receipt_df[project_receipt_df.len_project > 1].receipts.values)
    return receipts_with_projects_list


def split_by_projects(df_receipt_ct, small_project_df, verbose=True):
    """
    Return two Pandas DFs - with projects and without projects
    ---------------------
    wp - With Project, wop - WithOut Project, ct - Client Type
    """
    ct = df_receipt_ct.client_type.unique()[0]
    project_items_list = set().union(*small_project_df.comb_id_set.values)

    df_receipt_ct_listed = get_receipt_project_items_df(df_receipt_ct, project_items_list)
    receipts_with_projects_ct = get_receipts_with_projects_list(small_project_df, df_receipt_ct_listed)

    rare_receipts_mask_ct = df_receipt_ct.receipt_surrogate_id.isin(receipts_with_projects_ct)

    df_receipt_ct_wp = df_receipt_ct[rare_receipts_mask_ct].copy()
    df_receipt_ct_wp['project_type'] = 'WP'
    df_receipt_ct_wop = df_receipt_ct[-rare_receipts_mask_ct].copy()
    df_receipt_ct_wop['project_type'] = 'WOP'
    assert df_receipt_ct_wp.shape[0] + df_receipt_ct_wop.shape[0] == df_receipt_ct.shape[0]

    if verbose:
        print(f'N {ct} receipts Total:', df_receipt_ct.receipt_surrogate_id.nunique())
        print(f'N {ct} receipts with rare models / projects:', df_receipt_ct_wp.receipt_surrogate_id.nunique())
        print(f'N {ct} receipts with frequent models / without projects:',
              df_receipt_ct_wop.receipt_surrogate_id.nunique())
    return df_receipt_ct_wp, df_receipt_ct_wop


# ----------------------------------------------------------------------------------
#               GROUP UTILS
# ----------------------------------------------------------------------------------

def create_groups(df_receipt, df_items, min_sales=40, support_type='quantile', store_id=config.STORE_ID, log=False):
    # ------------------------------------
    # prep receipts
    # ------------------------------------
    client_type = df_receipt.client_type.unique()[0]
    project_type = df_receipt.project_type.unique()[0]

    start_time = time.time()

    # Group by receipt_surrogate_id and get unique comb_id values
    df_list_models = df_receipt.groupby('receipt_surrogate_id')['comb_id'].unique().reset_index(name='list_stockcode')

    # Filter for receipts with more than one comb_id
    df_list_models['n_comb_id'] = df_list_models['list_stockcode'].apply(len)
    df_list_models = df_list_models[df_list_models['n_comb_id'] > 1]

    print('N of comb_id:', df_receipt.comb_id.nunique())
    print('Total N of receipts:', df_list_models.shape[0])
    print('N of receipts with 1 comb_id:', df_list_models[df_list_models.n_comb_id == 1].shape[0])
    print('N of receipts with 2+ comb_id:', df_list_models.shape[0])

    if support_type == 'quantile':
        # Calculate the 90th percentile for item support (if support_type is 'quantile')
        all_items = np.concatenate(df_list_models['list_stockcode'].values)
        item_counts = pd.Series(all_items).value_counts(normalize=True)
        min_support = item_counts.quantile(0.8)  # Top 10% most frequent items
    else:
        # Use min_sales-based calculation for support
        min_support = round(min_sales / df_list_models.receipt_surrogate_id.nunique(), 6)

    print(f'min_support: {min_support}')

    dataset = df_list_models['list_stockcode'].values
    te = TransactionEncoder()
    te_ary = te.fit(dataset).transform(dataset)
    df = pd.DataFrame(te_ary, columns=te.columns_)

    # Frequent itemsets
    frequent_itemsets = fpgrowth(df, min_support=min_support, use_colnames=True)

    lift_th = 1.2
    arl_df = association_rules(frequent_itemsets, metric="lift", min_threshold=lift_th).sort_values('lift',
                                                                                                    ascending=False)

    print('Lift threshold:', lift_th)
    print('ARL DF shape:', arl_df.shape)

    # Create the group column by merging antecedents and consequent
    arl_df['group'] = arl_df.apply(lambda row: set(sorted(set(row['antecedents']).union(set(row['consequents'])))),
                                   axis=1)
    arl_df['len_g'] = arl_df['group'].apply(len)

    # Remove duplicates and reset index
    arl_df['group_str'] = arl_df['group'].apply(lambda x: str(sorted(list(x))))
    arl_df = arl_df.drop_duplicates(subset='group_str').reset_index(drop=True)

    print('ARL DF shape after group_str:', arl_df.shape)
    print('N groups:', arl_df.shape[0])

    # Assign group IDs
    arl_df['group_id'] = np.arange(1, arl_df.shape[0] + 1)

    # Assign main department
    arl_df['main_dep'] = arl_df['group'].apply(lambda group: get_main_department(group, df_items))

    # Count unique items in each group
    arl_df['n_items'] = arl_df['group'].apply(
        lambda group: len(df_items[df_items['comb_id'].isin(group)]['item'].unique()))

    # ------------------------------------
    # cleaning small, leaving large
    # ------------------------------------

    gs_th = arl_df['len_g'].max() - 1
    large_groups_list = arl_df[arl_df['len_g'] >= gs_th]['group'].values

    intersecting_groups_id = set()
    for tmp_group in large_groups_list:
        tmp_deps = arl_df[arl_df['group'] == tmp_group]['main_dep'].values[0]
        mask = arl_df['main_dep'] == tmp_deps
        arl_df_tmp = arl_df[mask][['group_id', 'group', 'len_g']]
        arl_df_tmp['intersec'] = arl_df_tmp['group'].apply(lambda group: len(set(tmp_group).intersection(group)))
        intersecting_groups_id.update(arl_df_tmp[arl_df_tmp['len_g'] == arl_df_tmp['intersec']]['group_id'].values)

    intersecting_groups_id = intersecting_groups_id.difference(set(arl_df[arl_df['len_g'] >= gs_th]['group_id']))

    # Filter out intersecting groups
    arl_df = arl_df[~arl_df['group_id'].isin(intersecting_groups_id)]

    arl_df['client_type'] = client_type
    final_group_cols = ['group', 'group_id', 'client_type', 'len_g', 'main_dep', 'n_items', 'support', 'confidence',
                        'lift', 'leverage', 'conviction', 'group_str']
    df_group = arl_df[final_group_cols].copy()

    # Remove duplicate group_str and finalize the data
    df_group['group_str'] = df_group['group'].apply(lambda x: str(sorted(list(x))))
    df_group = df_group.drop_duplicates(subset='group_str')
    df_group = df_group.drop(columns='group_str')

    df_group['len_g'] = df_group['group'].apply(len)

    print('Final DF group shape:', df_group.shape)
    stop_time = time.time()
    total_exec_time = round(stop_time - start_time, 1)
    group_month = (config.ED_CUR + dt.timedelta(1)).strftime('%h %y')

    log_data = [store_id, client_type, project_type,
                df_receipt['receipt_surrogate_id'].nunique(), df_receipt['comb_id'].nunique(),
                min_support, min_sales,
                df_group.shape[0], arl_df['len_g'].max(),
                total_exec_time, group_month]

    if log:
        return df_group, log_data
    else:
        return df_group


def get_main_department(group, df_items):
    """ Helper function to determine the main department of a group """
    vc_series = df_items[df_items['comb_id'].isin(group)]['dept'].value_counts()
    if len(vc_series) == 1 or vc_series.iloc[0] > vc_series.iloc[1]:
        return str(vc_series.index[0])
    elif vc_series.iloc[0] == vc_series.iloc[1]:
        return '_'.join(map(str, vc_series.index[:2]))
    return ''


def get_final_group_df(df_group_b2b_wp, df_group_b2b_wop, df_group_b2c_wp, df_group_b2c_wop,
                       store_id=config.STORE_ID):
    df_group_wp = pd.concat([df_group_b2b_wp, df_group_b2c_wp],
                            ignore_index=True)
    df_group_wp['receipt_type'] = 'With Project'
    df_group_wp['group_str'] = [str(sorted(list(g))) for g in df_group_wp.group.values]
    df_group_wp = df_group_wp.drop_duplicates('group_str')

    df_group_wop = pd.concat([df_group_b2b_wop, df_group_b2c_wop],
                             ignore_index=True)
    df_group_wop['receipt_type'] = 'Without Project'
    df_group_wop['group_str'] = [str(sorted(list(g))) for g in df_group_wop.group.values]
    df_group_wop = df_group_wop.drop_duplicates('group_str')

    df_group = pd.concat([df_group_wp, df_group_wop],
                         ignore_index=True)

    df_group['group_id'] = df_group.group_str.map({g: n + 1 for n, g in enumerate(df_group.group_str.unique())})
    df_group['store_id'] = store_id
    return df_group


def create_log_df():
    return pd.DataFrame(columns=['store_id', 'client_type', 'project_type',
                                 'n_receipts', 'n_comb_id', 'supp', 'min_sales', 'n_gr', 'max_gr_len', 'time',
                                 'period'])


def load_log_df(fpath=config.LOGS_DF_PATH):
    return pd.read_csv(fpath)


def add_to_log_df(log_df, log_list):
    return pd.concat([log_df,
                      pd.DataFrame(pd.Series(log_list,
                                             index=log_df.columns)).transpose()])


def evaluate_min_support_adaptive(min_support, df_list_models):
    """
    """
    min_support = max(min_support, 0.001)

    dataset = df_list_models.list_stockcode.values
    te = TransactionEncoder()
    te_ary = te.fit(dataset).transform(dataset)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)

    frequent_items_sets = fpgrowth(df_encoded, min_support=min_support, use_colnames=True)
    n_sets = frequent_items_sets.shape[0]

    avg_size = frequent_items_sets.itemsets.apply(len).mean() if n_sets > 0 else 0

    score = -abs(avg_size - 5)

    print(f"Trying min_support = {min_support:.6f}, Found {n_sets} sets, Avg size = {avg_size:.2f}, Score = {score}")

    return score


def optimize_min_support_adaptive(df_list_models):
    """
    """
    optimizer = BayesianOptimization(
        f=lambda min_support: evaluate_min_support_adaptive(min_support, df_list_models),
        pbounds={"min_support": (0.001, 0.1)},  # min_support от 0.1% до 10%
        random_state=42
    )

    optimizer.maximize(init_points=5, n_iter=10)  # 5 случайных точек, 10 итераций

    best_support = optimizer.max["params"]["min_support"]
    print(f"Optimal min_support: {best_support:.6f}")

    return best_support


def plot_min_support_impact(df_list_models):
    """
    Строит график зависимости числа частых наборов от min_support.
    """
    dataset = df_list_models.list_stockcode.values
    te = TransactionEncoder()
    te_ary = te.fit(dataset).transform(dataset)
    df_encoded = pd.DataFrame(te_ary, columns=te.columns_)

    support_values = np.linspace(0.001, 0.05, 10)  # 10 значений от 0.1% до 5%
    num_sets = []

    for min_support in support_values:
        frequent_items_sets = fpgrowth(df_encoded, min_support=min_support, use_colnames=True)
        num_sets.append(frequent_items_sets.shape[0])

    plt.figure(figsize=(8, 5))
    plt.plot(support_values, num_sets, marker='o', linestyle='-')
    plt.xlabel("min_support")
    plt.ylabel("Число частых наборов")
    plt.title("Влияние min_support на количество найденных наборов")
    plt.grid()
    plt.show()