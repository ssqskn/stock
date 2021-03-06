#coding:utf-8
from src.python.api.StockData import StockData
from src.python.utils.sql_engine import SqlEngine
from config.sql_account import BIGDEAL_VOL
import tushare.util.dateu as du
import threading, threadpool
import pandas as pd
import tushare as ts
import urllib
import time
import re


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
            data = data.drop_duplicates().reset_index(drop=True)
            data[["code", "name"]].to_sql("stock_list", self.engine, if_exists="replace", index=False)
            self.session.execute("alter table stock_list add primary key(code(6));")   ##增加主键约束
        except Exception as e:
            print("error when fetching stock list", e)        
        print("End fetching stock list with %ss used" % round(time.time()-tic, 1))
    
    def __fetch_stock_basics(self):
        """ 获取股票基本信息 """
        try:
            data = ts.get_stock_basics()
            data["op_day"] = du.today()
            data = data.fillna("")
            data = data.drop_duplicates().reset_index(drop=True)
            data.to_sql("stock_basics", self.engine, if_exists="replace", index=False)
        except Exception as e:
            print("error when fetching stock basics", e)
        print("End fetching stock basics with %ss used" % round(time.time()-tic, 1))

    def __fetch_stock_classification(self):
        """ 获取股票的各项分类数据 """
        table_map = {
                             "cl_industry" : {"func":ts.get_industry_classified, "uq_key":"uq_cli_code_name(code(6), c_name(30))"},
                             "cl_concept":  {"func":ts.get_concept_classified, "uq_key":"uq_clc_code_name(code(6), c_name(30))"},
                             "cl_area":  {"func":ts.get_area_classified, "uq_key":"uq_cla_code(code(6))"},
                             "cl_sme":  {"func":ts.get_sme_classified, "uq_key":"uq_clsme_code(code(6))"},
                             "cl_gem":  {"func":ts.get_gem_classified, "uq_key":"uq_clg_code(code(6))"},
                             "cl_st":  {"func":ts.get_st_classified, "uq_key":"uq_clst_code(code(6))"},
                             "cl_terminated":  {"func":ts.get_terminated, "uq_key":"uq_clt_code(code(6))"},
                             "cl_suspended":  {"func":ts.get_suspended, "uq_key":"uq_clsus_code(code(6))"}
                            }
        for table_name, value in table_map.items():
            try:
                data = value["func"]()
                data["op_day"] = self.today
                data = data.fillna("")
                data = data.drop_duplicates().reset_index(drop=True)
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
                self.session.execute("alter table %s add unique key %s;" % (table_name, value["uq_key"]))
            except Exception as e:
                print("error when fetching %s" %table_name, e)        
            print("End fetching %s with %ss used" % (table_name, round(time.time()-tic, 1)))

    def __fetch_stock_operating_data(self):
        """ 获取股票的运营数据，包括主报表、盈利能力、运营能力、成长能力、偿债能力、现金流量各项指标 """
        table_map = {
                             "op_report_data" : {"func":ts.get_report_data, "uq_key":"uq_opr_code_qrt(code(6), quarter(10))"},
                             "op_profit_data": {"func":ts.get_profit_data, "uq_key":"uq_opp_code_qrt(code(6), quarter(10))"},
                             "op_operation_data": {"func":ts.get_operation_data, "uq_key":"uq_opo_code_qrt(code(6), quarter(10))"},
                             "op_growth_data": {"func":ts.get_growth_data, "uq_key":"uq_opg_code_qrt(code(6), quarter(10))"},
                             "op_debtpaying_data": {"func":ts.get_debtpaying_data, "uq_key":"uq_opd_code_qrt(code(6), quarter(10))"},
                             "op_cashflow_data": {"func":ts.get_cashflow_data, "uq_key":"uq_opc_code_qrt(code(6), quarter(10))"}
                            }
        yy, quarter = self.last_two_quarter()
        for table_name, value in table_map.items():
            try:
                data = value["func"](yy, quarter)
                data["quarter"] = str(yy) + "-" + str(quarter)
                data["op_day"] = self.today
                data = data.drop_duplicates().reset_index(drop=True)
                data = data.fillna("")
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
                self.session.execute("alter table %s add unique key %s;" % (table_name, value["uq_key"]))
            except Exception as e:
                print("error when fetching %s" %table_name, e)
            print("End fetching %s with %ss used" % (table_name, round(time.time()-tic, 1)))

    def __fetch_economic_data(self):
        """ 获取宏观经济数据 """
        table_map = {
                             "eco_deposit_rate" : {"func":ts.get_deposit_rate, "uq_key":"uq_eco1(date(10), deposit_type(100))"},
                             "eco_loan_rate" : {"func":ts.get_loan_rate, "uq_key":"uq_eco2(date(10), loan_type(100))"},
                             "eco_reserve_deposit_ratio": {"func":ts.get_rrr, "uq_key":"uq_eco3(date(10))"},
                             "eco_money_supply": {"func":ts.get_money_supply, "uq_key":"uq_eco4(month(10))"},
                             "eco_gdp_quarter": {"func":ts.get_gdp_quarter, "uq_key":"uq_eco5(quarter)"},
                             "eco_cpi": {"func":ts.get_cpi, "uq_key":"uq_eco6(month(10))"},
                             "eco_ppi": {"func":ts.get_ppi, "uq_key":"uq_eco7(month(10))"},
                             "eco_gdp_for": {"func":ts.get_gdp_for, "uq_key":"uq_eco8(year)"},
                             "eco_gdp_pull": {"func":ts.get_gdp_pull, "uq_key":"uq_eco9(year)"},
                             "eco_gdp_contrib": {"func":ts.get_gdp_contrib, "uq_key":"uq_eco10(year)"},
                             "eco_shibor_data": {"func":ts.shibor_data, "uq_key":"uq_eco11(date)"},
                             "eco_lpr_data": {"func":ts.lpr_data, "uq_key":"uq_eco12(date)"}
                            }
        for table_name, value in table_map.items():
            try:
                data = value["func"]()
                data["op_month"] = str(self.year) + "-" + str(self.month)
                data = data.fillna("")
                data = data.drop_duplicates().reset_index(drop=True)
                data.to_sql(table_name, self.engine, if_exists="replace", index=False)
                self.session.execute("alter table %s add unique key %s;" % (table_name, value["uq_key"]))
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
                data = data.fillna("")
                if idx == 0:
                    data.to_sql("hist_trading_day", self.engine, if_exists="fail", index=False)
                else:
                    data.to_sql("hist_trading_day", self.engine, if_exists="append", index=False)
                print("---%s: success in fetching histrory data of code %s" % (idx, code))
            except Exception as e:
                print("---%s: error in fetching histrory data of code %s" % (idx, code), e)
        self.session.execute("alter table hist_trading_day add unique key hist_day_1(code(6), date(10));")
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
        
        for _ in range(int(row_count // batch_size) + 1):
            result = result_cursor.fetchmany(size=batch_size)
            threads = threadpool.makeRequests(fetch_bigdeal_one, result)
            for t in threads:
                pool.putRequest(t)
            pool.wait()
        result_cursor.close()
        print("End fetching history trading bigdeal data with time %ss" % round(time.time()-tic, 1))

    def __fetch_shareholderstrading(self):
        """获取股东持股数据"""
        url = "http://data.eastmoney.com/DataCenter_V3/gdzjc.ashx?" +\
                 "pagesize=500&page=PAGEN"+\
                 "&js=var%20VNAYjVCO&param=&sortRule=-1&sortType=BDJZ&tabid=all&code=&name=&rt=49552046"
        data = []
        for i in range(1, 100):
            for _ in range(3):       ##错误重试
                try:
                    respond_data = urllib.request.urlopen(url.replace("PAGEN", str(i)), timeout=20).read()
                    respond_data = respond_data.decode("gbk").replace("\"]}","")  ##清除尾部字符
                    respond_data = re.sub(r"var.+?\[\"", "", respond_data)             ##清除头部字符
                    records = respond_data.split("\",\"")
                    break
                except Exception as e:
                    print("error: ", e)
                    time.sleep(1)
            if len(records) < 2: break
            else:
                for rec in records:
                    data.append(rec.split(",")) 
            time.sleep(1)     ##休眠1秒，防止被网站屏蔽
            print("--page:%s, records: %s--" % (i, len(data)))
        data = pd.DataFrame(data, columns=["code", "name", "price", "change", "shareholder", "ch_flag", "ch_quant", \
                                                                  "ch_ratiofloat", "place", "tot_quant", "tot_ratiototal", "tot_quantfloat",\
                                                                  "tot_ratiofloat", "startday", "endday", "announceday", "ch_ratiototal"])
        data = data.drop_duplicates(["code","shareholder", "ch_flag", "ch_quant", "tot_quant", "announceday"]).reset_index(drop=True)
        print("End fetching trading data of major shareholders with time %ss, total page:%s, total records:%s" \
                 % (round(time.time()-tic, 1), str(i), len(data)))
        data["op_day"] = self.today
        data.to_sql("hist_shareholder_trading", self.engine, if_exists="replace", index=False)
        self.session.execute("alter table hist_shareholder_trading add unique key" +\
                                      "hist_shareholder_1(code(6), shareholder(50), ch_flag(10), ch_quant(15), tot_quant(15), announceday(10));")
           
    def fetch_all(self, is_first_time=False):
        """ 获取所有数据 """
        ##TODO: is_first_time需要通过检查数据库中是否有表来判断
        if is_first_time:
            self.__fetch_stock_list()
            self.__fetch_stock_basics()
            self.__fetch_stock_classification()
            self.__fetch_stock_operating_data()
            self.__fetch_economic_data()
            self.__fetch_shareholderstrading()
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
    is_first_time = False
    
    initializer = StockDataInitializer()
    initializer.fetch_all(is_first_time=is_first_time)
    
    
