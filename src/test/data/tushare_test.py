#coding:utf-8
import tushare as ts

show_all = False

if show_all:
    ## 基本面数据
    print(ts.get_stock_basics())        ##股票列表           目前不能访问：timeout
    print(ts.get_report_data(2016,3))    ##主报表
    print(ts.get_profit_data(2016,3))     ##盈利能力
    print(ts.get_operation_data(2016,3))    ##运营能力
    print(ts.get_growth_data(2016,3))    ##成长能力
    print(ts.get_debtpaying_data(2016,3))    ##偿债能力
    print(ts.get_cashflow_data(2016,3))    ##现金流量
    
    ##交易数据
    print(ts.get_index())              ##指数当前数据
    print(ts.get_hist_data('600848'))      ##历史股价
    print(ts.get_k_data(code='600068', start="2000-01-01"))     ##历史股价_接口2
    print(ts.get_tick_data('600848',date='2017-01-09'))     ##分笔数据
    print(ts.get_sina_dd('600848', date='2017-01-09', vol=300))      ##大单数据
    
    ##股票分类数据
    print(ts.get_industry_classified())     ##行业分类
    print(ts.get_concept_classified())     ##概念分类
    print(ts.get_area_classified())           ##地域分类
    print(ts.get_sme_classified())           ##中小板分类
    print(ts.get_gem_classified())          ##创业板分类
    print(ts.get_st_classified())              ##ST股票
    print(ts.get_terminated())               ##退市股票
    print(ts.get_suspended())               ##暂停上市股票

    ##投资参考数据
    ## TO BE TESTED
    
    ##龙虎榜数据
    ## TO BE TESTED

    ##宏观经济数据
    print(ts.get_deposit_rate())               ##存款利率
    print(ts.get_loan_rate())                    ##贷款利率
    print(ts.get_rrr())                              ##存款准备金率
    print(ts.get_money_supply())            ##货币供应量
    print(ts.get_money_supply_bal())      ##货币供应量（年底余额）
    print(ts.get_gdp_year())                    ##生产总值（全年）
    print(ts.get_gdp_quarter())               ##生产总值（季度）
    print(ts.get_cpi())                             ##CPI
    print(ts.get_ppi())                             ##工业品出厂价格指数
    print(ts.get_gdp_for())                      ##三大需求对GDP贡献率
    print(ts.get_gdp_pull())                     ##三大产业拉动率
    print(ts.get_gdp_contrib())               ##三大产业贡献率

    ##银行同业拆借利率
    print(ts.shibor_data())         ##shibor银行同业拆借利率
    print(ts.shibor_quote_data())      ##银行间报价
    print(ts.lpr_data())                     ##贷款基础利率

    ##新闻数据
    print(ts.get_latest_news(top=5,show_content=True))     ##即时新闻
    print(ts.get_notices())                                                     ##信息地雷
    print(ts.guba_sina(True))                                                       ##新浪股吧

    ##通联数据
    
    ##电影票房数据

    ##其他

    

