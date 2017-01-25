#coding:utf8
from sqlalchemy import create_engine
from config.sql_account import SQL_USERNAME, SQL_PASSWORD, SQL_URL, SQL_DBNAME

class SqlEngine():
    """
         创建单例模式的SQL连接引擎
    """
    sql_engine = None
    
    @classmethod
    def engine(cls):
        if cls.sql_engine is None:
            try:
                eng =  create_engine('mysql+pymysql://'+SQL_USERNAME+":"  \
                                                                +SQL_PASSWORD+"@" \
                                                                +SQL_URL+"/" \
                                                                +SQL_DBNAME +"?charset=utf8")
            except Exception as e:
                print("Error when creating SQL engine")
                raise Exception(e)
            cls.sql_engine = eng
        return cls.sql_engine


    
    







