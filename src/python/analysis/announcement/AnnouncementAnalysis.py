#coding:utf-8

from src.python.analysis.announcement.index_function import *


def announcement_analysis(file_path, year):
    """
    *  
    *  @param file_path: 分析的年报数据文件，必须是AnnouncementDataDownloader下载的文件 str
    """
        
    data = pd.read_csv(file_path, encoding="gbk")
    data_b1y = pd.read_csv(file_path.replace(str(year), str(year-1)), encoding="gbk")
    
    # 计算经营现金流净额占净利润比
    data["经营现金流净额占净利润比"] = get_operating_cash_div_net_profit(data)
    
    # 计算费用占毛利润比
    data["费用占毛利润比"] = get_expense_div_gross_profit(data)
    
    # 计算自由现金流
    
    
    ### 计算安全性
    data["流动比率"] = get_current_ratio(data)
    data["速动比率"] = get_quick_ratio(data)
    data["资产负债率"] = get_debt_div_assets(data)
    data["净资产负债率"] = get_debt_div_net_worth(data)
    
    ### 计算盈利能力
    ##(1) 收入角度
    data["营业利润率"] = get_operating_profit_rate(data)
    data["毛利率"] = get_gross_profit_rate(data)
    data["净利率"] = get_net_profit_rate(data)
    ##(2) 资产角度
    data["roe"] = get_roe(data, data_b1y)     ##净资产收益率
    data["总资产收益率"] = get_all_capital_earnings_rate(data, data_b1y)
    
    ## 计算成长性
    data["营业收入增长率"] = get_growth_rate(data, data_b1y, col_name="营业收入(元)")
    data["营业利润增长率"] = get_growth_rate(data, data_b1y, col_name="营业利润(元)")
    data["总资产增长率"] = get_growth_rate(data, data_b1y, col_name="资产总计(元)")
    data["净资产增长率"] = get_growth_rate(data, data_b1y, col_name="股东权益合计(元)")
    
    ## 计算营运能力
    data["应收账款周转率"] = get_turnover_ratio_of_receivable(data, data_b1y)
    data["存货周转率"] = get_turnover_ratio_of_inventory(data, data_b1y)
    data["固定资产周转率"] = get_turnover_ratio_of_fixed_assets(data, data_b1y)
    data["总资产周转率"] = get_turnover_ratio_of_total_assets(data, data_b1y)
    
    list_stock_info(data, code=600519)
    #list_stock_info(data, code=600519, part='balance')
    #list_stock_info(data, code=600519, part='profit')
    #list_stock_info(data, code=600519, part='cash')
    
    draw_plot_of_index(file_dir="D:/zzz/app/EclipseWorkspace/stock/data/", 
            start_year=2011, end_year=2015, code=600519, col_name="营业收入(元)")
    draw_plot_of_index(file_dir="D:/zzz/app/EclipseWorkspace/stock/data/", 
            start_year=2011, end_year=2015, code=600519, col_name="roe", func=get_roe)
    draw_plot_of_index(file_dir="D:/zzz/app/EclipseWorkspace/stock/data/", 
            start_year=2011, end_year=2015, code=600519, col_name="净利率", func=get_net_profit_rate)

    

    

if __name__ == "__main__":
    year = 2015
    file_path = "D:/zzz/app/EclipseWorkspace/stock/data/announcement_sh_%s.csv" % year
    announcement_analysis(file_path=file_path, year=year)

