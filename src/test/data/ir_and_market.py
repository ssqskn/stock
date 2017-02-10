#coding:utf-8
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

url = "http://value500.com/Bond.asp"
pattern = r"\<chart\>.+?\</chart\>"

response = urlopen(url, timeout=10).read()
result = re.findall(pattern, response.decode("utf-8"))
if len(result) > 0:
    response = BeautifulSoup(result[0])
    response_time = response.find_all("series")[0]
    response_value_gb = response.find_all("graph", title="到期:中国固定利率国债收益率曲线:10Y")[0]
    response_value_sh = response.find_all("graph", title="上证指数")[0]
    data_time = [i.string for i in response_time.find_all("value")]
    data_gb = [i.string for i in response_value_gb.find_all("value")]
    data_sh = [i.string for i in response_value_sh.find_all("value")]
    data = pd.DataFrame([[i,j,k] for i, j, k in zip(data_time, data_gb, data_sh)],
                                      columns = ["date", "gov_bond_10y", "sh_m"])
    data.to_csv("../../../results/gov_bond_10y.csv", index=False)
    print ("End fetching govement bond ratio")
    
        
        


