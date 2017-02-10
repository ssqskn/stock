#coding:utf-8
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re

url = "http://value500.com/PE.asp"
pattern = r"\<chart\>.+?\</chart\>"

response = urlopen(url, timeout=10).read()
result = re.findall(pattern, response.decode("utf-8"))
if len(result) > 0:
    response = BeautifulSoup(result[0])
    response_time = response.find_all("series")[0]
    response_value_sh = response.find_all("graph", title="上证A股市盈率")[0]
    response_value_sz = response.find_all("graph", title="深证A股市盈率")[0]
    data_time = [i.string for i in response_time.find_all("value")]
    data_sh = [i.string for i in response_value_sh.find_all("value")]
    data_sz = [i.string for i in response_value_sz.find_all("value")]
    data = pd.DataFrame([[i,j,k] for i, j, k in zip(data_time, data_sh, data_sz)],
                                      columns = ["date", "pe_sh", "pd_sz"])
    data.to_csv("../../../results/market_pe.csv", index=False)
    print ("End fetching market PEs")
    
        
        
        


