#coding:utf8
from src.python.utils.sql_engine import SqlEngine
import pandas as pd

ins1 = SqlEngine.engine()
ins2 = SqlEngine.engine()
assert ins1 == ins2

df = pd.DataFrame([1,2,3,4], columns =["aaa"])
df.to_sql("temp", SqlEngine.engine())

for i in dir(ins1): 
    print(i)