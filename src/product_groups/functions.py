import re
import pandas as pd
import numpy as np
import psycopg2
import os
import queries as q

from clickhouse_driver import Client
from tqdm import tqdm
from dotenv import load_dotenv


def get_connection(bd):
    load_dotenv('a.env')
    if bd == "ch":

        connection_ch = os.getenv("host_ch")
        user_ch = os.getenv("user_ch")
        password_ch = os.getenv("password_ch")
        return (connection_ch, user_ch, password_ch)

    if bd == "gp":
        connection = f"""host={os.getenv("host")}
            port={os.getenv("port")}
            dbname={os.getenv("dbname")} 
            user={os.getenv("user")}
            password={os.getenv("password")}"""
        return connection


def get_data_gp(SQL: str, CONNECTION: str) -> pd.DataFrame:
    """
    Получение данных из  БД GreenPlum
    :param CONNECTION:

    """
    with psycopg2.connect(CONNECTION) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            columns = [column[0] for column in cur.description]
            df = pd.DataFrame(data=cur.fetchall(), columns=columns)
    return df


def read_input_data(input_data_path):
    if input_data_path.endswith("csv"):
        return pd.read_csv(input_data_path)
    else:
        return pd.read_excel(input_data_path)


def prepare_model_groups_df(df, group_col="group"):
    df[group_col] = [
        set(a.replace("{", "").replace("}", "").replace(",", "").replace("'", "").split()) for a in df[group_col].values
    ]
    return df


def create_comb_id(df):
    return (
        df.model_id.astype(str)
        + "_"
        + df.dept.astype(str)
        + "_"
        + df.subdept.astype(str)
        + "_"
        + df.class_id.astype(str)
        + "_"
        + df.subclass.astype(str)
    )


def prepare_group_df(group_data_path):
    df_group_start = read_input_data(group_data_path)
    if "group_id" in df_group_start.columns:
        # model groups
        df_group_start = prepare_model_groups_df(df_group_start)
        df_group = df_group_start[["group_id", "group"]].drop_duplicates("group_id").copy().reset_index(drop=True)

    else:
        # item groups
        # main_cols[0] - group_id column, [1] - group itself
        main_cols = ["id группы", "Артикул"]
        df_group = (
            df_group_start.groupby(main_cols[0])[main_cols[1]]
            .apply(set)
            .reset_index()
            .rename(columns={main_cols[0]: "group_id", main_cols[1]: "group"})
        )
    return df_group


def estim_group_sales(df_group, df_receipt, n_sales_th=0, method=""):
    """
    Parameters
    ----------
    df_group : pd.DataFrame with group_id and group of items/models
    df_receipt : pd.DataFrame with receipts
    n_sales_th : int
                 Min N sales of the group (sales strictly greater than n_sales_th)
                 Default 0
    Returns
    -------

    Pandas DataFrame with group_id, item combinations (for model groups only) and N sales for each group_id

    """
    if method == "":
        if isinstance(df_group.group_id.values[0], str):
            items = True
        else:
            items = False
    elif method == "sku":
        items = True
    elif method == "model":
        items = False
    else:
        print("Unknown method")
    # добавил условие для принудительного выбора метода

    if items:
        df_receipt_list = (
            df_receipt.sort_values(by=["receipt_surrogate_id", "line_item_id"])
            .groupby("receipt_surrogate_id")
            .line_item_id.apply(set)
            .reset_index(name="list_items")
        )
        # добавил агрегацию из многострочных записей группы артикулов в одну запись
        df_group = df_group.groupby("group_id").item.apply(set).reset_index(name="group")
    else:
        df_receipt["comb_id"] = create_comb_id(df_receipt)
        df_receipt_list = (
            df_receipt.sort_values(by=["receipt_surrogate_id", "line_item_id"])
            .groupby("receipt_surrogate_id")
            .comb_id.apply(set)
            .reset_index(name="list_items")
        )
        df_receipt_list["n_comb_id"] = [len(c) for c in df_receipt_list.list_items.values]
        df_receipt_list = df_receipt_list[df_receipt_list.n_comb_id > 1]

    group_id_list, receipt_list = [], []

    for group_id in tqdm(df_group.group_id.unique(), desc="Searcing for groups in receipts"):
        group_id_list.append(group_id)
        group = df_group[df_group.group_id == group_id].group.values[0]
        mask_groups = df_receipt_list.list_items.map(group.issubset)
        receipt_list.append(df_receipt_list[mask_groups].receipt_surrogate_id.values)

    df_group_rec = pd.DataFrame({"group_id": group_id_list, "receipts": receipt_list})
    if items:
        # drop groups without sales
        df_group_rec["n_sales"] = [len(r) for r in df_group_rec.receipts.values]
        df_group_rec = df_group_rec[df_group_rec.n_sales > n_sales_th]
        #  изменил возвращаемый df
        return (
            df_group_rec[["group_id", "n_sales"]]
            .merge(df_group[["group_id", "group"]], on="group_id", how="left", validate="1:1")
            .rename(columns={"group": "item_combs"})
        )
    else:
        df_group_rec = df_group_rec.explode("receipts")
        df_group_rec = df_group_rec.groupby("receipts").group_id.unique().reset_index()

        df_receipt = df_receipt.merge(
            df_group_rec.rename(columns={"receipts": "receipt_surrogate_id"}),
            on="receipt_surrogate_id",
            how="left",
            validate="m:1",
        )
        df_receipt = df_receipt[df_receipt.group_id.notnull()]
        df_receipt = df_receipt.explode("group_id")

        df_group_short = df_group[["group_id", "group"]].copy()
        df_group_short["group"] = [list(g) for g in df_group_short.group.values]
        df_group_short = df_group_short.explode("group")

        # all pairs group_id-comb_id gotta be unique()
        # assert df_group_short.drop_duplicates().shape[0] == df_group_short.shape[0]
        df_group_short.drop_duplicates(inplace=True)
        df_receipt = df_receipt.merge(
            df_group_short.rename(columns={"group": "comb_id"}), on=["group_id", "comb_id"], how="inner", validate="m:1"
        )
        df_n_sales = (
            df_receipt.groupby(["group_id", "receipt_surrogate_id"])
            .line_item_id.unique()
            .reset_index(name="item_combs")
        )
        df_n_sales["item_combs"] = [str(sorted(items_list)) for items_list in df_n_sales.item_combs.values]
        df_n_sales = (
            df_n_sales.groupby(["group_id", "item_combs"]).receipt_surrogate_id.count().reset_index(name="n_sales")
        )
        df_n_sales = df_n_sales[df_n_sales.n_sales > n_sales_th].reset_index(drop=True)
        df_n_sales["item_combs"] = [
            [int(item) for item in list_el.replace("[", "").replace("]", "").replace(",", "").split()]
            for list_el in tqdm(df_n_sales["item_combs"].values)
        ]
        return df_n_sales


def normalize_groups(df, method="model", items_col="item_combs", logs=False):
    """
    Приводим группы к виду comb_id - item - n_sales
    """
    if logs: 
        print("Normalizing groups..")
    
    if method == "model":
        df_groups = pd.DataFrame(columns=["comb_id", "item", "n_sales"])
        comb_id = 0
        if type(df[items_col].iloc[0]) == str:
            for row in tqdm(range(len(df))):
                n_sales = df["n_sales"].iloc[row]
                for item in list(map(int, df[items_col].iloc[row][1:-1].split(", "))):
                    df_groups.loc[len(df_groups.index)] = [str(comb_id), item, n_sales]
                comb_id += 1
        else:
            for row in tqdm(range(len(df))):
                n_sales = df["n_sales"].iloc[row]
                for item in df[items_col].iloc[row]:
                    df_groups.loc[len(df_groups.index)] = [str(comb_id), item, n_sales]
                comb_id += 1
    if method == "sku":
        if type(df[items_col].iloc[0]) == str:
            for row in tqdm(range(len(df))):
                n_sales = df["n_sales"].iloc[row]
                for item in list(map(int, df[items_col].iloc[row][1:-1].split(", "))):
                    df_groups.loc[len(df_groups.index)] = [comb_id, item, n_sales]
                comb_id += 1
        else:
            for row in tqdm(range(len(df))):
                n_sales = df["n_sales"].iloc[row]
                for item in df[items_col].iloc[row]:
                    df_groups.loc[len(df_groups.index)] = [comb_id, item, n_sales]
                comb_id += 1
    return df_groups


def add_group_info(df_groups, df_items, logs=False):
    """
    Цепляем информацию к группам
    Мейн отдел, кол-во отделов,
    кол-во подотделов, кол-во моделей,
    длина группы
    """
    if logs: 
        print("Append info for groups..")
    assert set(["comb_id", "item"]).issubset(set(df_groups.columns))
    # merge info
    df_groups = df_groups.merge(df_items, on="item", how="left", validate="m:1")
    # add main_dept
    df_groups = df_groups.merge(
        df_groups.groupby(["comb_id", "dept"])
        .agg({"item": "count"})
        .reset_index()
        .sort_values(by=["comb_id", "item"], ascending=[True, False])
        .drop_duplicates(subset="comb_id", keep="first")
        .rename(columns={"dept": "main_dept"})[["comb_id", "main_dept"]],
        on="comb_id",
        how="left",
        validate="m:1",
    )
    # uniq depts, subdepts, models in group
    df_groups["depts_amt"] = df_groups.groupby("comb_id")["dept"].transform("nunique")
    df_groups["subdepts_amt"] = df_groups.groupby("comb_id")["subdept"].transform("nunique")
    df_groups["models_amt"] = df_groups.groupby("comb_id")["model_id"].transform("nunique")
    # groups length
    df_groups["gr_len"] = df_groups.groupby("comb_id")["item"].transform("count")
    return df_groups


def many_md_to_one(df):
    """
    Вытаскивает из групп на моделях одиночные модели
    !!! потеряла актуальность
    """
    print("Preparing solo models..")
    one_md = pd.DataFrame(columns=["group", "group_id"])
    id = 0
    for row in tqdm(df["group"]):
        models = re.sub("'", "", row[1:-1]).split(", ")
        for model in models:
            value = "{" + f"{model}" + "}"
            one_md.loc[len(one_md.index)] = [value, id]
            id += 1
    return one_md.drop_duplicates(subset="group")


def groups_pipeline(df, df_receipt, df_items, min_sales=5, min_len=3, method="sku"):
    """
    1 функция для вызова последовательности функций
    !!! пока что не доработана после обновы логики, лучше не юзать
    """
    print("solo md part count..")
    df_1 = (
        add_group_info(
            normalize_groups(
                estim_group_sales(
                    prepare_model_groups_df(many_md_to_one(df)), df_receipt, n_sales_th=min_sales - 1, method=method
                )
            ),
            df_items,
        )
        .query("gr_len >= @min_len")
        .sort_values(by=["n_sales", "comb_id"], ascending=[False, True])
    )
    print("many models part count..")
    df_2 = (
        add_group_info(
            normalize_groups(
                estim_group_sales(prepare_model_groups_df(df), df_receipt, n_sales_th=min_sales - 1, method=method)
            ),
            df_items,
        )
        .query("gr_len >= @min_len")
        .sort_values(by=["n_sales", "comb_id"], ascending=[False, True])
    )
    print("concat..")
    last_index = df_2["comb_id"].astype("int").max()
    df_1["comb_id"] = df_1["comb_id"].astype("int") + last_index + 2
    return pd.concat([df_2, df_1])


def reset_group_index(df_group_sales):
    """
    Если сделали группы на много магазинов, получим одинаковые группы
    с разными id
    С помощью этой функции получим универсальные id для одинаковых группы
    Одинаковые идентифицируем с помощью сборки в список отсортированных артикулов и дроп дубликатов
    """
    df_group_sales.sort_values(by=["comb_id", "item"], ascending=[True, True], inplace=True)
    items_combs = (
        df_group_sales.groupby(["comb_id"])["item"].apply(list).reset_index().rename(columns={"item": "items_comb"})
    )
    df_group_sales = df_group_sales.merge(items_combs, on="comb_id", how="left", validate="m:1")
    df_group_sales["items_comb"] = df_group_sales["items_comb"].astype("str")
    uniq_groups = df_group_sales["items_comb"].unique()

    df_group_sales = (
        df_group_sales.merge(
            pd.DataFrame({"new_id": [x for x in range(len(uniq_groups))], "items_comb": uniq_groups}),
            on="items_comb",
            how="left",
            validate="m:1",
        )
        .drop(["items_comb", "comb_id"], axis=1)
        .rename(columns={"new_id": "comb_id"})
    )

    new_id = df_group_sales["comb_id"]
    df_group_sales.drop(labels=["comb_id"], axis=1, inplace=True)
    df_group_sales.insert(0, "comb_id", new_id)

    return df_group_sales.drop_duplicates()


def group_sales_turnover(group: pd.DataFrame, start_date, end_date, stores: list, receipts=None):
    """
    Только для одной группы!
    Чеки должны содержать только итемы заданной группы
    """
    assert len(group["comb_id"].unique()) == 1

    """
    Если не передали чеки, достаем их
    из базы, оставляю только записи с артикулами из группы
    """

    if receipts is None:
        receipts = get_data_ch(
            q.get_group_receipts(
                stores=stores, items=list(group["item"].unique()), start_date=start_date, end_date=end_date
            ),
            *get_connection("ch"),
        )
    else:
        list_items = list(group["item"].unique())
        receipts = receipts.query("item in @list_items")

    """
    Считаем кол-во уникальных итемов группы
    в каждом чеке-магазине. Там же суммируем ТО и маржу
    Затем по каждому кол-ву итемов группируем и получаем
    кол-во чеков, маржу и ТО для каждого кол-ва итемов группы в продаже
    """
    turnover_group_df = (
        receipts.groupby(["receipt_surrogate_id", "store_id"])
        .agg({"item": "nunique", "line_turnover": "sum", "line_margin": "sum"})
        .reset_index()
        .groupby(["store_id", "item"])
        .agg({"line_turnover": "sum", "line_margin": "sum", "receipt_surrogate_id": "count"})
        .reset_index()
        .sort_values(by=["store_id", "item"], ascending=[True, False])
    )
    turnover_group_df["comb_id"] = group["comb_id"].iloc[0]

    return turnover_group_df[
        ["comb_id", "store_id", "item", "line_turnover", "line_margin", "receipt_surrogate_id"]
    ].rename(columns={"item": "items", "receipt_surrogate_id": "receipts"})


def group_status(turnover_df):
    """
    Считает соотношения порядков ТО соло и группового для моделей
    Порядок ТО группы > порядок ТО продаж без группы этих артикулов -> best
    При равном порядке -> good
    При меньшем -> bad, но заполняется только постфактум через fillna, вне в функции
    """
    good_groups = pd.DataFrame(columns=["comb_id", "store_id", "to_status"])
    for group in tqdm(turnover_df["comb_id"].unique()):
        for store in turnover_df[turnover_df["comb_id"] == group]["store_id"].unique():
            if (
                turnover_df[(turnover_df["comb_id"] == group) & (turnover_df["store_id"] == store)]["receipts"].iloc[0]
                > 0
            ):
                solo = (
                    turnover_df[(turnover_df["comb_id"] == group) & (turnover_df["store_id"] == store)]
                    .sort_values(by="items", ascending=True)["line_turnover"]
                    .iloc[0]
                )
                together = (
                    turnover_df[(turnover_df["comb_id"] == group) & (turnover_df["store_id"] == store)]
                    .sort_values(by="items", ascending=False)["line_turnover"]
                    .iloc[0]
                )

                if len(str(int(solo)).lstrip("0")) == len(str(int(together)).lstrip("0")):
                    good_groups.loc[len(good_groups.index)] = [group, store, "good"]

                elif len(str(int(solo)).lstrip("0")) < len(str(int(together)).lstrip("0")):
                    good_groups.loc[len(good_groups.index)] = [group, store, "best"]
    return good_groups


def group_receipts(df_receipt, n_days=7, short=True):
    df_receipt_days = df_receipt[["client_card", "receipt_surrogate_id", "opened_date"]].drop_duplicates()

    df_receipt_days = df_receipt_days.sort_values(by=["client_card", "opened_date"], ascending=[True, True])
    df_receipt_days["receipts_on_client"] = df_receipt_days.groupby("client_card")["receipt_surrogate_id"].transform(
        "count"
    )
    df_receipt_days = df_receipt_days[df_receipt_days.receipts_on_client > 1]
    df_receipt_days.dropna(inplace=True)

    df_receipt_days = df_receipt_days.sort_values(by=["client_card", "opened_date"], ascending=[True, True])

    df_receipt_days["days_after_prev"] = [
        (df_receipt_days["opened_date"].iloc[x] - df_receipt_days["opened_date"].iloc[x - 1]).days
        for x in range(len(df_receipt_days["opened_date"]))
    ]

    df_receipt_days["days_till_next"] = [
        (df_receipt_days["opened_date"].iloc[x + 1] - df_receipt_days["opened_date"].iloc[x]).days
        for x in range(len(df_receipt_days["opened_date"]) - 1)
    ] + [0]

    # заполняем первые покупки нулями
    df_receipt_days.loc[
        df_receipt_days.index.isin(df_receipt_days.drop_duplicates(subset="client_card", keep="first").index),
        "days_after_prev",
    ] = 0

    # заполняем последние покупки нулями
    df_receipt_days.loc[
        df_receipt_days.index.isin(df_receipt_days.drop_duplicates(subset="client_card", keep="last").index),
        "days_till_next",
    ] = 0

    zero_days_mask = (df_receipt_days.days_after_prev == 0) | (df_receipt_days.days_till_next == 0)
    same_date_sales = (
        df_receipt_days[zero_days_mask]
        .groupby(["client_card", "opened_date"])
        .receipt_surrogate_id.count()
        .reset_index(name="n_sales")
    )
    same_date_sales = same_date_sales[same_date_sales.n_sales > 1]

    df_receipt_sameday = df_receipt_days.merge(
        same_date_sales, on=["client_card", "opened_date"], how="right", validate="m:1"
    )

    days_mask = (
        ((df_receipt_days.days_after_prev == 0) & (df_receipt_days.days_till_next <= n_days))
        | (
            (df_receipt_days.days_after_prev != 0)
            & (df_receipt_days.days_after_prev <= n_days)
            & (df_receipt_days.days_till_next != 0)
        )
        | (
            ((df_receipt_days.days_after_prev != 0))
            & (df_receipt_days.days_till_next != 0)
            & (df_receipt_days.days_till_next <= n_days)
        )
        | ((df_receipt_days.days_after_prev <= n_days) & (df_receipt_days.days_till_next == 0))
    )

    df_receipt_days_filtered = df_receipt_days[days_mask].copy()

    del df_receipt_sameday["n_sales"]
    df_receipt_res = (
        pd.concat([df_receipt_days_filtered, df_receipt_sameday], ignore_index=True)
        .drop_duplicates()
        .sort_values(["client_card", "opened_date"], ignore_index=True)
    )

    df_receipt_res = (
        df_receipt_res.sort_values(["client_card", "opened_date"]).reset_index().rename(columns={"index": "id_index"})
    )
    index_groupid_dict = {}

    df_receipt_res["days"] = [
        d1 if d1 <= n_days else d2 for d1, d2 in zip(df_receipt_res.days_after_prev, df_receipt_res.days_till_next)
    ]

    id_rec = 1
    for i in tqdm(range(len(df_receipt_res)), desc="groupping receipts"):
        i_rec = df_receipt_res.iloc[i].id_index
        if i != 0:
            if df_receipt_res.iloc[i - 1].client_card != df_receipt_res.iloc[i].client_card:
                id_rec += 1
        if i != len(df_receipt_res) - 1:
            if df_receipt_res.iloc[i].days <= n_days and df_receipt_res.iloc[i].days_till_next <= n_days:
                index_groupid_dict[i_rec] = id_rec
                continue
            elif df_receipt_res.iloc[i].days_till_next > n_days:
                index_groupid_dict[i_rec] = id_rec
                id_rec += 1
        else:
            index_groupid_dict[i_rec] = id_rec

    df_receipt_res["rec_group_id"] = df_receipt_res.id_index.map(index_groupid_dict)
    del df_receipt_res["id_index"]

    if short:
        return df_receipt_res[["client_card", "receipt_surrogate_id", "opened_date", "rec_group_id"]]
    else:
        return df_receipt_res


def create_group(items: list, df_items: pd.DateOffset, receipts=None):
    if receipts is not None:
        temp_sales = (
            receipts.query("line_item_id in @items")
            .groupby("receipt_surrogate_id")
            .agg({"line_item_id": "nunique"})
            .reset_index()
            .groupby("line_item_id")
            .agg({"receipt_surrogate_id": "nunique"})
            .reset_index()
            .sort_values(by="line_item_id", ascending=False)
        )
        if temp_sales["line_item_id"].iloc[0] == len(items):
            sales = temp_sales["receipt_surrogate_id"].iloc[0]
        else:
            sales = 0

        return add_group_info(
            normalize_groups(pd.DataFrame({"group_id": 0, "item_combs": str(items), "n_sales": sales}, index=[0])),
            df_items
        )
    else:
        return add_group_info(
            normalize_groups(pd.DataFrame({"group_id": 0, "item_combs": str(items), "n_sales": np.nan}, index=[0])),
            df_items
        )
