#coding:utf8
from src.python.utils.sql_engine import SqlEngine
from sqlalchemy.orm import sessionmaker

class StockData(object):
    """
        数据获取类的基类，所有涉及数据获取的类需继承此类，以确保
        日期及其他参数的一致
    """
    engine = None
    session = None
    
    today = None
    year = None
    month = None
    quarter = None
    
    def __init__(self):
        self.engine = SqlEngine.engine()
        self.session = sessionmaker(bind=self.engine)()
        from tushare.util import dateu as du
        self.today = du.today()
        self.year, self.month = int(self.today.split("-")[0]), int(self.today.split("-")[1])
        self.quarter = int((int(self.month) + 2) / 3)
    
    def last_quarter(self):
        if self.quarter == 1:
            return int(self.year)-1, 4
        else:
            return self.year, self.quarter-1
    
    def last_two_quarter(self):
        if self.quarter == 1:
            return int(self.year)-1, 4
        elif self.quarter == 2:
            return int(self.year)-1, 3
        else:
            return self.year, self.quarter-2
    
    

        
        
        