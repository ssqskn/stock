#coding:utf-8
from src.python.api.StockData import StockData
from src.python.utils.sql_engine import SqlEngine
from src.python.utils.utils import add_md5_col
from config.sql_account import BIGDEAL_VOL
import tushare.util.dateu as du
import threading, threadpool
import tushare as ts
import time

tic = time.time()
counter_for_bigdeal = 0
n_records_for_bigdeal = 0
lock = threading.Lock()

class StockDataInitializer(StockData):
    """
        获取新的股票数据，
        第一次获取股票数据时使用
    """    
    def __init__(self):
        super(StockDataInitializer, self).__init__()

    def __fetch_stock_list(self):
        """ 获取股票列表 """
        try:
            data = ts.get_area_classified()
            data[["code", "name"]].to_sql("stock_list", self.engine, if_exists="replace", index=False)
        except Exception as e:
            print("error when fetching stock list", e)        
        print("End fetching stock list with %ss used" % round(time.time()-tic, 1))
    
    def __fetch_stock_basics(self):
        """ 获取股票基本信息 """
        try:
            data = ts.get_stock_basics()
            data["op_day"] = du.today()
            data.to_sql("stock_basics", self.engine, if_exists="replace", index=False)
        except Exception as e:
            print("error when fetching stock basics", e)
        print("End fetching stock basics with %ss used" % round(time.time()-tic, 1))

    def __fetch_stock_classification(self):
        """ 获取股票的各项分类数据 """
        table_map = {
                             "cl_industry" : ts.get_industry_classified,
                             "cl_concept": ts.get_concept_classified,
                             "cl_area": ts.get_area_classified,
                             "cl_sme": ts.get_sme_classified,
                             "cl_gem": ts.get_gem_classified,
                             "cl_st": ts.get_st_classified,
                             "cl_terminated": ts.get_terminated,
                             "cl_suspended": ts.get_suspended
                            }
        for table_name, func in table_map.items():
            try:
                data = func()
                data_md5 = add_md5_col(data)   ##add md5 column
                data["op_day"] = self.today
                data["md5_without_op_day"] = data_md5
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
            except Exception as e:
                print("error when fetching %s" %table_name, e)        
            print("End fetching %s with %ss used" % (table_name, round(time.time()-tic, 1)))

    def __fetch_stock_operating_data(self):
        """ 获取股票的运营数据，包括主报表、盈利能力、运营能力、成长能力、偿债能力、现金流量各项指标 """
        table_map = {
                             "op_report_data" : ts.get_report_data,
                             "op_profit_data": ts.get_profit_data,
                             "op_operation_data": ts.get_operation_data,
                             "op_growth_data": ts.get_growth_data,
                             "op_debtpaying_data": ts.get_debtpaying_data,
                             "op_cashflow_data": ts.get_cashflow_data
                            }
        for table_name, func in table_map.items():
            try:
                data = func(self.year, self.quarter)
                data["quarter"] = str(self.year) + "-" + str(self.quarter)
                data_md5 = add_md5_col(data)   ##add md5 column
                data["op_day"] = self.today
                data["md5_without_op_day"] = data_md5
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
            except Exception as e:
                print("error when fetching %s" %table_name, e)
            print("End fetching %s with %ss used" % (table_name, round(time.time()-tic, 1)))

    def __fetch_economic_data(self):
        """ 获取宏观经济数据 """
        table_map = {
                             "eco_deposit_rate" : ts.get_deposit_rate,
                             "eco_loan_rate" : ts.get_loan_rate,
                             "eco_reserve_deposit_ratio": ts.get_rrr,
                             "eco_money_supply": ts.get_money_supply,
                             "eco_gdp_quarter": ts.get_gdp_quarter,
                             "eco_cpi": ts.get_cpi,
                             "eco_ppi": ts.get_ppi,
                             "eco_gdp_for": ts.get_gdp_for,
                             "eco_gdp_pull": ts.get_gdp_pull,
                             "eco_gdp_contrib": ts.get_gdp_contrib,
                             "eco_shibor_data": ts.shibor_data,
                             "eco_lpr_data": ts.lpr_data
                            }
        for table_name, func in table_map.items():
            try:
                data = func()
                data_md5 = add_md5_col(data)    ##add md5 column
                data["op_month"] = str(self.year) + "-" + str(self.month)
                data["md5_without_op_day"] = data_md5
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
            except Exception as e:
                print("error when fetching %s" %table_name, e)        
            print("End fetching %s with %ss used" % (table_name, round(time.time()-tic, 1)))

    def __fetch_trading_data(self):
        """ 获取所有股票历史交易数据，从1990-01-01开始获取 """
        def _fetch_one(code, retry_count=3, pause=0.01):
            """ 获取一支股票从1990-01-01开始的所有日交易数据，
                 因为默认日期的接口最多只返回两年数据，所以要每两年拆分一条语句进行获取"""
            data = None
            for i in range(1990, self.year+1 , 2):
                start, end = str(i)+"-01-01", str(i+1) + "-12-31"
                try:
                    data_split = ts.get_k_data(code=code, start=start, end=end, retry_count=3,  pause=0.1)
                    if data is None:
                        data = data_split
                    else:
                        data = data.append(data_split)
                except ValueError:
                    continue
            data = data.reset_index(drop=True)
            return data
        
        ##TODO: 用多线程进行数据获取
        ##TODO: 按照(code, date)配对有重复，需核查(mysql中用group by检查下)
        
        stock_code = ts.get_area_classified()["code"].values.tolist()
        for idx, code in enumerate(stock_code):
            try:
                data = _fetch_one(code=code)
                data["op_day"] = self.today
                if idx == 0:
                    data.to_sql("hist_trading_day", self.engine, if_exists="fail", index=False)
                else:
                    data.to_sql("hist_trading_day", self.engine, if_exists="append", index=False)
                print("---%s: success in fetching histrory data of code %s" % (idx, code))
            except Exception as e:
                print("---%s: error in fetching histrory data of code %s" % (idx, code), e)
        print("End fetching trading data with %ss used" % round(time.time()-tic, 1))

    def __fetch_bigdeal_all(self, start, end, batch_size=1000, n_threads=50):
        """ 获取大单数据 """
        pool = threadpool.ThreadPool(n_threads)
        try:
            result_cursor = self.session.execute("select code, date from hist_trading_day \
                                                                    where date > '%s' and date < '%s'" %(start, end))
            row_count = result_cursor.rowcount
            assert row_count > 0
            print("End loading (code, date) pairs from hist_trading_day with %ss" % round(time.time()-tic, 1))
        except Exception as e:
            print("error when loading code, date from hist_trading_day", e)
            raise Exception(e)
        
        for _ in range(row_count / batch_size + 1):
            result = result_cursor.fetchmany(size=batch_size)
            threads = threadpool.makeRequests(fetch_bigdeal_one, result)
            for t in threads:
                pool.putRequest(t)
            pool.wait()
        result_cursor.close()
        print("End fetching history trading bigdeal data with time %ss" % round(time.time()-tic, 1))

    def fetch_all(self, is_first_time=False):
        """ 获取所有数据 """
        ##TODO: is_first_time需要通过检查数据库中是否有表来判断
        if is_first_time:
            self.__fetch_stock_list()
            self.__fetch_stock_basics()
            self.__fetch_stock_classification()
            self.__fetch_stock_operating_data()
            self.__fetch_economic_data()
            self.__fetch_trading_data()
            self.__fetch_bigdeal_all(start="2016-12-28", end=self.today)
        
        
def fetch_bigdeal_one(rec):
    """ 获取一条大单数据 """
    ## 多线程中可以用这种方式操作共享变量，多进程中不能用这种方式
    global counter_for_bigdeal, n_records_for_bigdeal, lock
    with lock:
        counter_for_bigdeal += 1
        if counter_for_bigdeal % 500 == 0:
            print(counter_for_bigdeal, "\t" , round(time.time()-tic, 1), "s\t", n_records_for_bigdeal)
    
    try:
        code, date = str(rec[0]), str(rec[1])
        data = ts.get_sina_dd(code=code, date=date, vol=BIGDEAL_VOL, retry_count=3, pause=0.1)
    except Exception as e:
        data = None
        print("error when fetching bigdeal data of %s" % code, e)
    if data is not None:
        data["day"] = str(date)
        data["op_day"] = du.today()
        try:
            lock.acquire()
            n_records_for_bigdeal += len(data)
            data.to_sql("hist_trading_bigdeal", SqlEngine.engine(), if_exists="append", index=False)
        except Exception as e:
            print("error when writing bigdeal data to sql of %s" % code, e)
        finally:
            lock.release()



if __name__ == "__main__":
    ## 全部数据提取的flag
    is_first_time = True
    
    initializer = StockDataInitializer()
    initializer.fetch_all(is_first_time=is_first_time)
    
    
