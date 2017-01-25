#coding:utf-8
from hashlib import md5
import datetime as dt

def add_md5_col(data):
    """
        DataFrame中加入MD5列
        @param data DataFrame  数据集
        @return data_md5 list  每条记录的md5值的列表
    """
    cols = list(data.columns)
    for idx, tp in enumerate(data.dtypes):
        if str(tp).lower().startswith("float") or str(tp).lower().startswith("double"):
            data[cols[idx]] = data[cols[idx]].astype(str)
        elif str(tp).lower().startswith("long") or str(tp).lower().startswith("int"):
            data[cols[idx]] = data[cols[idx]].astype(str)
        elif str(tp).lower().startswith("datetime"):
            data[cols[idx]] = data[cols[idx]].apply(lambda x: x.strftime("%Y-%m-%d")).astype(str)
        elif str(tp).lower().startswith("object"):
            try:  data[cols[idx]] = data[cols[idx]].astype(str)
            except UnicodeEncodeError: continue
        else:
            continue
        
    try:
        data_md5 = [md5("".join(rec)).hexdigest() for rec in data.values]
    except:
        data_md5 = [md5("".join(rec).encode("utf-8")).hexdigest() for rec in data.values]
    assert len(data_md5) > 0
    return data_md5


def forward_ndays(date, n_days):
    date = dt.datetime.strptime(date, "%Y-%m-%d")
    date = date + dt.timedelta(days=n_days)
    return str(dt.datetime.strftime(date, "%Y-%m-%d"))

