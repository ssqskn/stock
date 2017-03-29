#coding:utf-8


def str_to_money(series):
    """
    *  把带逗号分隔符的货币字符串转换成float型
    *  @param series: pandas的Series类型
    """
    return series.apply(lambda x: float(str(x).replace(",", "")) if x != '-' else 0.0)


def get_growth_rate(data, data_b1y, col_name):
    """
    *  计算指标增长率: 本年某指标值 / 前一年某指标值
    *  @param data: 当年的年报数据 DataFrame
    *  @param data_b1y: 前一年的年报数据 DataFrame
    *  @param col_name: 计算增长率的指标列名 str
    """
    suffixes = ["", "_b1y"]
    col_2years = data[["code", col_name]].merge(data_b1y[["code",col_name]],
            left_on="code", right_on="code", how="left", suffixes=suffixes)
    col_2years[col_name+suffixes[1]] = col_2years[col_name+suffixes[1]].fillna("-999")
    return str_to_money(col_2years[col_name]) / str_to_money(col_2years[col_name+suffixes[1]]) - 1


def get_average(data, data_b1y, col_name):
    """
    *  计算指标的年度平均值: (本年某指标值 + 前一年某指标值) / 2
    *  @param data: 当年的年报数据 DataFrame
    *  @param data_b1y: 前一年的年报数据 DataFrame
    *  @param col_name: 计算平均值的指标列名 str
    """
    suffixes = ["", "_b1y"]
    col_2years = data[["code", col_name]].merge(data_b1y[["code",col_name]],
            left_on="code", right_on="code", how="left", suffixes=suffixes)
    return (str_to_money(col_2years[col_name]) + str_to_money(col_2years[col_name+suffixes[1]])) * 0.5


def get_roe(data, data_b1y):
    """
    *  ROE: 净资产收益率 = 税后利润 / 平均净资产
    """
    return str_to_money(data["净利润(元)"]) / get_average(data, data_b1y, col_name="股东权益合计(元)")

def get_all_capital_earnings_rate(data, data_b1y):
    """
    *  总资产收益率: 税后利润 / 平均总资产
    """
    return str_to_money(data["净利润(元)"]) / get_average(data, data_b1y, col_name="资产总计(元)")

def get_gross_profit_rate(data):
    """
    *  毛利率: (主营业务收入 - 主营业务成本) / (主营业务收入)
    """
    return 1 - str_to_money(data["营业成本(元)"]) / str_to_money(data["营业收入(元)"])
    
def get_operating_profit_rate(data):
    """
    *  营业利润率: 营业利润 / 营业收入
    """
    return str_to_money(data["营业利润(元)"]) / str_to_money(data["营业收入(元)"])

def get_net_profit_rate(data):
    """
    *  净利率: 净利润 / 主营业务收入
    """
    return str_to_money(data["净利润(元)"]) / str_to_money(data["营业收入(元)"])

def get_operating_cash_div_net_profit(data):
    """
    *  经营现金流净额占净利润比: 经营现金流净额 / 净利润
    """
    return str_to_money(data["经营活动现金流量净额(元)"]) / str_to_money(data["净利润(元)"])

def get_expense_div_gross_profit(data):
    """
    *  费用占毛利润比: (销售费用 + 管理费用 + 财务费用) / 毛利润
    """
    #TODO: 是否要包含财务费用？
    return (str_to_money(data["销售费用(元)"]) + str_to_money(data["管理费用(元)"]) + str_to_money(data["财务费用(元)"]))\
         / (str_to_money(data["营业收入(元)"]) - str_to_money(data["营业成本(元)"]))
    
def get_current_ratio(data):
    """
    *  流动比率: 流动资产 / 流动负债
    """
    return str_to_money(data["流动资产合计(元)"]) / str_to_money(data["流动负债合计(元)"])
    
def get_quick_ratio(data):
    """
    *  速动比率: 速动资产 / 流动负债， 速动资产 = 流动资产 - 存货
    """
    return (str_to_money(data["流动资产合计(元)"]) - str_to_money(data["存货(元)"])) / str_to_money(data["流动负债合计(元)"])
    
def get_debt_div_assets(data):
    """
    *  资产负债率: 负债 / 资产
    """
    return str_to_money(data["负债合计(元)"]) / str_to_money(data["资产总计(元)"])
    
def get_debt_div_net_worth(data):
    """
    *  净资产负债率: 负债 / 所有者权益
    """
    return str_to_money(data["负债合计(元)"]) / str_to_money(data["股东权益合计(元)"])

def get_turnover_ratio_of_receivable(data, data_b1y):
    """
    *  应收账款周转率: 营业收入 / 平均应收账款
    """
    return str_to_money(data["营业收入(元)"]) / get_average(data, data_b1y, col_name="应收帐款(元)")

def get_turnover_ratio_of_inventory(data, data_b1y):
    """
    *  存货周转率: 营业收入 / 存货平均余额
    """
    return str_to_money(data["营业收入(元)"]) / get_average(data, data_b1y, col_name="存货(元)")

def get_turnover_ratio_of_fixed_assets(data, data_b1y):
    """
    *  固定资产周转率: 营业收入 / 平均固定资产
    """
    return str_to_money(data["营业收入(元)"]) / get_average(data, data_b1y, col_name="固定资产净额(元)")

def get_turnover_ratio_of_total_assets(data, data_b1y):
    """
    *  总资产周转率: 营业收入 / 平均总资产
    """
    return str_to_money(data["营业收入(元)"]) / get_average(data, data_b1y, col_name="资产总计(元)")



