#coding:utf-8
from src.python.data.StockDataInitializer import fetch_bigdeal_one
from src.python.api.StockData import StockData
from src.python.utils.utils import forward_ndays
import threadpool
import tushare as ts
import urllib
import time
import re


tic = time.time()

class StockDataUpdater(StockData):
    """
        股票数据更新，需在StockDataInitializer.fetch_all()调用完成之后使用
        @param start_day str 更新起始日期，格式：YYYY-MM-DD
    """
    #TODO: 对没有retry参数的接口，要自定义重试功能
    
    start_day = None

    def __init__(self, start_day=None):
        super(StockDataUpdater, self).__init__()
        if start_day is not None:
            self.start_day = start_day
        else:
            self.start_day = self.session.execute("select max(date) from hist_trading_day").first()[0]
        assert self.start_day is not None
        self.start_day = str(self.start_day)
    
    def __get_op_day(self, table_name):
        sql = "select max(op_day) from %s" % table_name
        op_day = self.session.execute(sql).first()[0]
        assert op_day is not None
        return str(op_day)

    
    def update_stock_list(self):
        """ 更新股票列表 """
        try:
            data = ts.get_area_classified()
            values = ",".join([str(tuple(i)) for i in data[["code", "name"]].values])
            self.session.execute("replace into stock_list values %s; commit;" % values)
        except Exception as e:
            print("error when fetching stock list", e)
        print("End updating stock list with %ss used" % round(time.time()-tic, 1))

    def update_stock_basics(self):
        """ 更新股票基本信息 """
        last_op_day = self.__get_op_day("stock_basics")
        if last_op_day == self.today:
            print("already up-to-date")
            return None
        try:
            data = ts.get_stock_basics()
            data["op_day"] = self.today
            data = data.fillna("")
            data.to_sql("stock_basics", self.engine, if_exists="append", index=False)
        except Exception as e:
            print("error when updating stock basics", e)
        print("End updating stock basics with %ss used" % round(time.time()-tic, 1))

    def update_stock_classification(self):
        """ 更新股票分类数据 """
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
                data["op_day"] = self.today
                data = data.fillna("")
                if len(data) > 0:
                    values = ",".join([str(tuple(i)) for i in data.values])
                    self.session.execute("insert ignore into %s values %s; commit;" % (table_name, values))
                print("---updated table: %s with %ss used---" % (table_name, round(time.time()-tic, 1)))
            except Exception as e:
                print("error when fetching %s  with %ss used" %(table_name, round(time.time()-tic, 1)), e)

    def update_stock_operating_data(self):
        """ 更新股票的运营数据"""
        table_map = {
                             "op_report_data" : ts.get_report_data,
                             "op_profit_data": ts.get_profit_data,
                             "op_operation_data": ts.get_operation_data,
                             "op_growth_data": ts.get_growth_data,
                             "op_debtpaying_data": ts.get_debtpaying_data,
                             "op_cashflow_data": ts.get_cashflow_data
                            }
        for yy, quarter in [self.last_two_quarter(), self.last_quarter()]:
            for table_name, func in table_map.items():
                try:
                    data = func(yy, quarter)
                    data["quarter"] = str(yy) + "-" + str(quarter)
                    data["op_day"] = self.today
                    data = data.fillna("")
                    if len(data) > 0:
                        values = ",".join([str(tuple(i)) for i in data.values])
                        self.session.execute("insert ignore into %s values %s; commit;" % (table_name, values))
                    print("---updated table: %s with %ss used---" % (table_name, round(time.time()-tic, 1)))
                except Exception as e:
                    print("error when fetching %s" %table_name, e)
                        
    def update_economic_data(self):
        """ 更新宏观经济数据 """
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
                data["op_month"] = str(self.year) + "-" + str(self.month)
                data = data.fillna("")
                if len(data) > 0:
                    values = ",".join([str(tuple(i)) for i in data.values])
                    self.session.execute("insert ignore into %s values %s; commit;" % (table_name, values))
                print("---updated table: %s with %ss used---" % (table_name, round(time.time()-tic, 1)))
            except Exception as e:
                print("error when fetching %s  with %ss used" %(table_name, round(time.time()-tic, 1)), e)

    def update_trading_data(self):
        """ 
            更新所有股票历史交易数据，只支持更新两年内的数据 
            注：该函数依赖于cl_area表，更新前需首先完成cl_area表的更新
        """
        #stock_code = ts.get_area_classified()["code"].values.tolist()
        stock_code = [i[0] for i in self.session.execute("select code from cl_area").fetchall()]
        code_date_pair = self.session.execute("select code, date from hist_trading_day \
                                                                    where date >= '%s' group by code, date" %self.start_day).fetchall()
        data_all = None
        for idx, code in enumerate(stock_code):
            try: 
                data = ts.get_k_data(code=code, start=self.start_day, end=self.today, retry_count=3,  pause=0.01)
            except Exception as e: 
                print("---error when fetching deal data of code: %s" %code, e)
                continue
            data = data[[i not in code_date_pair for i in zip(data["code"], data["date"])]]
            if data_all is None:
                data_all = data.copy()
            else:
                data_all = data_all.append(data)
            if idx % 100 == 0:
                print("---%s stocks trading data fetched with %ss---" %(idx, round(time.time()-tic, 1)))
        data_all = data_all.reset_index(drop=True)
        data_all["op_day"] = self.today
        data_all = data_all.fillna("")
        if len(data_all) > 0:
            values = ",".join([str(tuple(i)) for i in data_all.values])
            self.session.execute("insert ignore into hist_trading_day values %s; commit;" % (values))
        print("---updated table: hist_trading_day with %ss used---" % (round(time.time()-tic, 1)))
   
    def update_bigdeal(self):
        """ 
            更新所有股票的大单交易数据 
            注：该函数依赖于hist_trading_day表，更新前需首先完成hist_trading_day表的更新
        """
        start_day_bd = forward_ndays(self.start_day, -7)
        code_date_pair = self.session.execute("select code, day from hist_trading_bigdeal \
                                                                    where day >= '%s'  group by code, day" %start_day_bd).fetchall()
        code_date_pair_new = self.session.execute("select code, date from hist_trading_day \
                                                                           where date >= '%s' group by code, date" %start_day_bd).fetchall()
        code_date_pair_new = [i for i in code_date_pair_new if i not in code_date_pair]
        print("Start fetching stock bigdeal data, total records:%s" % len(code_date_pair_new))
        
        global counter_for_bigdeal, n_records_for_bigdeal
        counter_for_bigdeal = 0
        n_records_for_bigdeal = 0
        pool = threadpool.ThreadPool(20)
        threads = threadpool.makeRequests(fetch_bigdeal_one, code_date_pair_new)
        for t in threads:
            pool.putRequest(t)
        pool.wait()
        print("End updating history trading bigdeal data with time %ss" % round(time.time()-tic, 1))
     
    def update_shareholdertrading(self):
        """更新股东持股数据"""
        url = "http://data.eastmoney.com/DataCenter_V3/gdzjc.ashx?" +\
                 "pagesize=500&page=PAGEN"+\
                 "&js=var%20VNAYjVCO&param=&sortRule=-1&sortType=BDJZ&tabid=all&code=&name=&rt=49552046"
        data = []
        for i in range(1, 7):
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
        if len(data) > 0:
            data = [i+[self.today] for i in data]
            values = ",".join([str(tuple(i)) for i in data])
            self.session.execute("insert ignore into hist_shareholder_trading values %s; commit;" % (values))
            print("---updated table: hist_shareholder_trading with %ss used---" % (round(time.time()-tic, 1)))
                
    def update_all(self, is_update=False):
        """ 更新所有数据 """
        if is_update:
            self.update_stock_list()
            self.update_stock_basics()
            self.update_stock_classification()
            self.update_economic_data()
            self.update_stock_operating_data()
            self.update_trading_data()
            self.update_bigdeal()
        self.update_shareholdertrading()
    
    
if __name__ == "__main__":
    is_update = False
    stockDataUpdater = StockDataUpdater()
    stockDataUpdater.update_all(is_update=is_update)

