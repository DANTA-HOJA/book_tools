import inspect
from copy import deepcopy
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import rich.progress
import tomlkit
from rich.console import Console
# -----------------------------------------------------------------------------/


def load_config(path: Path,
                console: Console):
    """
    """
    if not isinstance(path, Path):
        raise TypeError("Config should be a `Path` object, "
                            "using 'from pathlib import Path'")
    
    with rich.progress.open(path, mode="r",
                                encoding="utf-8") as f_reader:
        config = tomlkit.load(f_reader)
    
    console.print(f"Config: '{path.resolve()}'")
    
    return config
    # -------------------------------------------------------------------------/


def read_ws(wb_path: Path,
            ws_name: str, ws_dtype: dict[str, str],
            colidx: int, default_key: set,
            console: Console):
    """
    colidx 是 class attr
    default_key 是 class attr
    """
    # read work sheet
    with rich.progress.open(wb_path, "rb", description=ws_name) as f:
        ws_df = pd.read_excel(f, engine="openpyxl", sheet_name=ws_name,
                              header=colidx, dtype=ws_dtype)
    # col name check
    current_key = set(ws_df.keys())
    diff_key = default_key.symmetric_difference(current_key)
    if len(diff_key) > 0:
        console.print(f":warning: Warning : [工作表]'{ws_name}' 的 Column Name 與 default 不同, \n"
                      f"Default : {default_key}\n"
                      f"Current : {current_key}\n"
                      f"Difference : {diff_key}\n")
    # show info
    console.print(f"[工作表]'{ws_name}' row count : {len(ws_df.index)}")
    
    return ws_df
    # -------------------------------------------------------------------------/


# def set_df_dtype(df: pd.DataFrame, dtype:dict, colalias:dict):
#     """
#     """
#     for key in df.keys():
#         if key == "index":
#             pass
#         elif key in dtype:
#             df[key] = df[key].astype(dtype[key])
#         else:
#             df[key] = df[key].astype("string")
    
#     # ISBN
#     isbn = colalias["ISBN"]
#     df[isbn] = df[isbn].astype("Float64")
#     df[isbn] = df[isbn].astype("Int64")
#     df[isbn] = df[isbn].astype("string")
    
#     df[isbn] = np.where(pd.isna(df[isbn]), df[isbn], df[isbn].str.strip())
    
#     return df
#     # -------------------------------------------------------------------------/


def col_strip(df: pd.DataFrame, col_name: str):
    """
    """
    assert pd.api.types.is_string_dtype(df[col_name])
    # df[col_name] = df[col_name].apply(lambda x: x.strip()).astype("string")
    
    df[col_name] = np.where(pd.isna(df[col_name]), df[col_name], df[col_name].str.strip())
    df[col_name] = df[col_name].astype("string")
    # -------------------------------------------------------------------------/


def copy_as_index(df: pd.DataFrame, col_name: str, idx_name: str):
    """
    """
    df[idx_name] = df[col_name]
    
    df.set_index(idx_name, inplace=True)
    # -------------------------------------------------------------------------/


def val_match_check(col_name: str,
                    purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                    cata_filtered: pd.DataFrame, catalog_colalias: dict,
                    console:Console):
    """
    """
    try:
        purc_colidx_target = purc_filtered.columns.get_loc(col_name)
        cata_colidx_target = cata_filtered.columns.get_loc(col_name)
    except KeyError:
        purc_colidx_target = purc_filtered.columns.get_loc(purchasing_colalias[col_name])
        cata_colidx_target = cata_filtered.columns.get_loc(catalog_colalias[col_name])
    
    # 交貨清單
    purc_value = purc_filtered.iloc[0, purc_colidx_target]
    
    # 編目箱單
    cata_value = cata_filtered.iloc[0, cata_colidx_target]
    if col_name == "書名":
        cata_value = cata_value.split("/")[0].strip()
    
    # check value
    if purc_value != cata_value:
        # show msg
        console.print(f":warning: {col_name}不相等\n"
                      f"\t'交貨清單' : {purc_value}\n"
                      f"\t'編目箱單' : {cata_value}")
    # -------------------------------------------------------------------------/


def update_cata_value(cata_rp2_purc:dict,
                      purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                      cata_filtered: pd.DataFrame, catalog_colalias: dict,
                      console:Console):
    """
    """
    # copy as new df (independent)
    _purc_filtered: pd.DataFrame = deepcopy(purc_filtered)
    _cata_filtered: pd.DataFrame = deepcopy(cata_filtered)
    
    assert len(_purc_filtered) == 1, "len(_purc_filtered) != 1"
    assert len(_cata_filtered) == 1, "len(_cata_filtered) != 1"
    
    # check '書名' and 'ISBN'
    val_match_check("書名",
                    _purc_filtered, purchasing_colalias,
                    _cata_filtered, catalog_colalias,
                    console)
    val_match_check("ISBN",
                    _purc_filtered, purchasing_colalias,
                    _cata_filtered, catalog_colalias,
                    console)
    console.line()
    
    for k in cata_rp2_purc.keys():
        # action1: '書名' = '書名' + '部冊號'
        if k == "部冊號":
            # get idx for `df.iloc`
            purc_colidx_bookname = _purc_filtered.columns.get_loc("書名")
            cata_colidx_booksn = _cata_filtered.columns.get_loc(k)
            # update `booksn` after `bookname`
            if not pd.isna(_cata_filtered.iloc[0, cata_colidx_booksn]):
                _purc_filtered.iloc[0, purc_colidx_bookname] = \
                    (f"{_purc_filtered.iloc[0, purc_colidx_bookname]}"
                     f" - {_cata_filtered.iloc[0, cata_colidx_booksn]}")
        # action2: replace target values in "編目箱單" to "交貨清單"
        else:
            try:
                purc_colidx_target = _purc_filtered.columns.get_loc(k)
                cata_colidx_target = _cata_filtered.columns.get_loc(k)
            except KeyError:
                purc_colidx_target = _purc_filtered.columns.get_loc(purchasing_colalias[k])
                cata_colidx_target = _cata_filtered.columns.get_loc(catalog_colalias[k])
            _purc_filtered.iloc[0, purc_colidx_target] = \
                                _cata_filtered.iloc[0, cata_colidx_target]
        
    return _purc_filtered
    # -------------------------------------------------------------------------/


def case_normal(df: pd.DataFrame, cata_rp2_purc:dict,
                purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                cata_filtered: pd.DataFrame, catalog_colalias: dict,
                console:Console):
    """
    """
    tmp_df = update_cata_value(cata_rp2_purc,
                               purc_filtered, purchasing_colalias,
                               cata_filtered, catalog_colalias,
                               console)
    
    return pd.concat([df, tmp_df], ignore_index=True)
    # -------------------------------------------------------------------------/


def case_bookset(df: pd.DataFrame, cata_rp2_purc:dict,
                 purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                 cata_filtered: pd.DataFrame, catalog_colalias: dict,
                 console:Console):
    """
    """
    selected_colnames = [
        '序號', '採購序號', '原始序號', '箱號', '登錄號', '書名', '作者', '出版社', '出版年', 'ISBN',
        '館藏地代碼', '資料類型/特藏號', # '定價', '折扣價\n79折', '總冊數', '數量', '小計',
        '主題分類', '得獎/推薦1', '得獎/推薦2', '套書/複本', '書單來源', '分類號', # '冊數', '單位',
    ]
    
    # annotation row ("登錄號" column = "套書")
    tmp_df = deepcopy(purc_filtered)
    tmp_df["登錄號"] = "套書"
    df = pd.concat([df, tmp_df], ignore_index=True)
    
    for i in range(len(cata_filtered)):
        tmp_df = update_cata_value(cata_rp2_purc,
                                   purc_filtered, purchasing_colalias,
                                   pd.DataFrame([cata_filtered.iloc[i]]), catalog_colalias,
                                   console)
        df = pd.concat([df, tmp_df.loc[:, selected_colnames]], ignore_index=True)
    
    return df
    # -------------------------------------------------------------------------/


def case_bookcopy(df: pd.DataFrame, cata_rp2_purc:dict,
                  purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                  cata_filtered: pd.DataFrame, catalog_colalias: dict,
                  console:Console):
    """
    """
    selected_colnames = [
        '序號', '採購序號', '原始序號', '箱號', '登錄號', '書名', '作者', '出版社', '出版年', 'ISBN',
        '館藏地代碼', '資料類型/特藏號', # '定價', '折扣價\n79折', '總冊數', '數量', '小計',
        '主題分類', '得獎/推薦1', '得獎/推薦2', '套書/複本', '書單來源', '分類號', # '冊數', '單位',
    ]
    
    for i in range(len(cata_filtered)):
        tmp_df = update_cata_value(cata_rp2_purc,
                                   purc_filtered, purchasing_colalias,
                                   pd.DataFrame([cata_filtered.iloc[i]]), catalog_colalias,
                                   console)
        if i == 0:
            df = pd.concat([df, tmp_df], ignore_index=True)
        else:
            df = pd.concat([df, tmp_df.loc[:, selected_colnames]], ignore_index=True)
    
    return df
    # -------------------------------------------------------------------------/


# def case_difflibarea(df: pd.DataFrame, cata_rp2_purc:dict,
#                      purc_filtered: pd.DataFrame, purchasing_colalias: dict,
#                      cata_filtered: pd.DataFrame, catalog_colalias: dict,
#                      console:Console):
#     """
#     """
#     for i in range(len(purc_filtered)):
#         tmp_df = update_cata_value(cata_rp2_purc,
#                                    pd.DataFrame([purc_filtered.iloc[i]]), purchasing_colalias,
#                                    pd.DataFrame([cata_filtered.iloc[i]]), catalog_colalias,
#                                    console)
#         df = pd.concat([df, tmp_df], ignore_index=True)
    
#     return df
#     # -------------------------------------------------------------------------/


def show_row_info(pd_series: pd.Series, colalias: dict, console:Console):
    """
    """
    fn_name = inspect.currentframe().f_code.co_name
    assert isinstance(pd_series, pd.Series), \
        f"{fn_name}(), `pd_series` should be `pd.Series` but got {type(pd_series)}"
    
    purc_sn = pd_series[colalias["採購序號"]]
    isbn = pd_series[colalias["ISBN"]]
    bookname = pd_series[colalias["書名"]]
    console.print(f":mag_right: 採購序號 : {purc_sn:04}, "
                  f":globe_showing_asia-australia: ISBN : {isbn}, "
                  f":notebook_with_decorative_cover: 書名 : '{bookname}'")
    # -------------------------------------------------------------------------/


def msg_booknum_mismatch(purc_filtered: pd.DataFrame, purchasing_colalias: dict,
                         cata_filtered: pd.DataFrame, catalog_colalias: dict,
                         console:Console):
    """
    """
    if purc_filtered is not None:
        console.print(f":package: '交貨清單' : ")
        for i in range(len(purc_filtered)):
            show_row_info(purc_filtered.iloc[i], purchasing_colalias, console)
    if cata_filtered is not None:
        console.print(f":bookmark: '編目箱單' : ")
        for i in range(len(cata_filtered)):
            show_row_info(cata_filtered.iloc[i], catalog_colalias, console)
    # -------------------------------------------------------------------------/


def sync_catalog_value(df: pd.DataFrame, filter: str, cata_rp2_purc:dict,
                       purc_df: pd.DataFrame, purchasing_colalias: dict,
                       cata_df: pd.DataFrame, catalog_colalias: dict,
                       console:Console):
    """
    """
    # reset variables
    handled_type: str = None
    
    # filtering uid
    purc_filtered = deepcopy(purc_df[(purc_df.index == filter)])
    cata_filtered = deepcopy(cata_df[(cata_df.index == filter)])
    
    # Error 1: 交貨清單不是唯一值
    if len(purc_filtered) != 1:
        msg_booknum_mismatch(purc_filtered, purchasing_colalias,
                             None, catalog_colalias, console)
        raise ValueError("'交貨清單' filtering 後偵測到多筆資料")
    # show current book info
    show_row_info(purc_filtered.iloc[0], purchasing_colalias, console)
    console.line()
    
    if len(cata_filtered) == 0:
        console.print("[yellow]本次該書沒有交貨\n")
        handled_type = "沒有交貨"
        return df, handled_type
    
    # Error 2: '交貨清單' 總冊數 ≠ '編目箱單' 冊數
    if purc_filtered.iloc[0]["總冊數"] != len(cata_filtered):
        msg_booknum_mismatch(purc_filtered, purchasing_colalias,
                             cata_filtered, catalog_colalias, console)
        raise ValueError("'交貨清單' 總冊數 ≠ '編目箱單' 冊數")
    
    if (len(purc_filtered) == 1) and (len(cata_filtered) == 1):
        # Normal case
        df = case_normal(df, cata_rp2_purc,
                         purc_filtered, purchasing_colalias,
                         cata_filtered, catalog_colalias,
                         console)
        handled_type = "Normal Case"
    elif (len(purc_filtered) == 1) and (len(cata_filtered) > len(purc_filtered)):
        if purc_filtered.iloc[0]["冊數"] > 1:
            # 套書
            df = case_bookset(df, cata_rp2_purc,
                              purc_filtered, purchasing_colalias,
                              cata_filtered, catalog_colalias,
                              console)
            handled_type = "套書"
        elif purc_filtered.iloc[0]["數量"] > 1:
            # 副本
            df = case_bookcopy(df, cata_rp2_purc,
                               purc_filtered, purchasing_colalias,
                               cata_filtered, catalog_colalias,
                               console)
            handled_type = "副本"
        else:
            msg_booknum_mismatch(purc_filtered, purchasing_colalias,
                                 cata_filtered, catalog_colalias, console)
            raise ValueError("'交貨清單' 冊數(套書) or 數量(副本) 數值異常")
    else:
        raise ValueError("Unexpected Error")
    
    
    if handled_type is not None:
        purc_df.drop(purc_filtered.index, inplace=True)
        cata_df.drop(cata_filtered.index, inplace=True)
    
    return df, handled_type
    # -------------------------------------------------------------------------/


def add_total_sum(df: pd.DataFrame):
    """
    """
    total_book = df["總冊數"].sum()
    
    # '金額' 四捨五入使用 round() 可能會出錯，改用 `ROUND_HALF_UP`
    total_ntd = Decimal(df["小計"].sum())
    total_ntd = total_ntd.quantize(Decimal('1'), rounding=ROUND_HALF_UP)
    
    total_row = pd.DataFrame({
            "書名": ["合計"],
            "總冊數": [total_book],
            "小計": [total_ntd],
    })
    
    return pd.concat([df, total_row], ignore_index=True)
    # -------------------------------------------------------------------------/
