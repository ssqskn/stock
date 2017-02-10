#coding:utf-8
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

url = "http://value500.com/CPI1.asp"
pattern = r"\<chart\>.+?\</chart\>"

response = urlopen(url, timeout=10).read()
result = re.findall(pattern, response.decode("utf-8"))
if len(result) > 0:
    response = BeautifulSoup(result[0])
    response_time = response.find_all("series")[0]
    response_value_1 = response.find_all("graph", title="10年期国债收益率-CPI")[0]
    response_value_2 = response.find_all("graph", title="1年期存款利率-CPI")[0]
    response_value_sh = response.find_all("graph", title="上证指数")[0]
    data_time = [i.string for i in response_time.find_all("value")]
    data_1 = [i.string for i in response_value_1.find_all("value")]
    data_2 = [i.string for i in response_value_2.find_all("value")]
    data_sh = [i.string for i in response_value_sh.find_all("value")]
    data = pd.DataFrame([[i,j,k,m] for i, j,k,m in zip(data_time, data_1, data_2, data_sh)],
                                      columns = ["date", "gov_bond_10y_ir", "deposit_rate_1y_ir", "sh_m"])
    data.to_csv("../../../results/ir_and_market.csv", index=False)
    print ("End fetching IRs and market index")
    
        
        


