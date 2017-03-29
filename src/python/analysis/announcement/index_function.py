#coding:utf-8

import matplotlib.pyplot as plt
import pandas as pd
import seaborn
import inspect

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


def list_stock_info(data, code, part="all"):
    """
    *  列出股票的年报数据
    *  @param data 年报数据 DataFrame
    *  @param code 股票代码 int
    *  @param part 列出部分信息 string
    *              all - 全部, balance - 资产负债表, profit - 利润表, cash - 现金表
    """
    if part == 'all':
        header = list(data.columns)
    elif part == 'balance':
        header = [ "货币资金(元)", "结算备付金(元)", "拆出资金(元)", "交易性金融资产(元)", "应收票据(元)", "应收帐款(元)",
            "预付帐款(元)", "应收保费(元)", "应收分保账款(元)", "应收分保合同准备金(元)", "应收利息(元)", "应收股利(元)", "其他应收款(元)",
            "买入返售金融资产(元)", "存货(元)", "一年内到期的非流动资产(元)", "其他流动资产(元)", "流动资产合计(元)", "发放贷款和垫款(元)",
            "可供出售金融资产(元)", "持有至到期投资(元)", "长期应收款(元)", "长期股权投资(元)", "投资性房地产(元)", "固定资产净额(元)",
            "在建工程(元)", "工程物资(元)", "固定资产清理(元)", "生产性生物资产(元)", "油气资产(元)", "无形资产(元)", "开发支出(元)",
            "商誉(元)", "长期待摊费用(元)", "递延税款借项合计(元)", "其他长期资产(元)", "非流动资产合计(元)", "资产总计(元)",
            "短期借款(元)", "向中央银行借款(元)", "吸收存款及同业存放(元)", "拆入资金(元)", "交易性金融负债(元)", "应付票据(元)",
            "应付帐款(元)", "预收帐款(元)", "卖出回购金融资产款(元)", "应付手续费及佣金(元)", "应付职工薪酬(元)", "应交税金(元)",
            "应付利息(元)", "应付股利(元)", "其他应付款(元)", "应付分保账款(元)", "保险合同准备金(元)", "代理买卖证券款(元)",
            "代理承销证券款(元)", "一年内到期的长期负债(元)", "其他流动负债(元)", "流动负债合计(元)", "长期借款(元)", "应付债券(元)",
            "长期应付款(元)", "专项应付款(元)", "预计负债(元)", "递延税款贷项合计(元)", "其他长期负债(元)", "长期负债合计(元)",
            "负债合计(元)", "股本(元)", "资本公积(元)", "库存股(元)", "盈余公积(元)", "一般风险准备(元)", "未分配利润(元)",
            "外币报表折算差额(元)", "归属于母公司所有者权益合计(元)", "少数股东权益(元)", "股东权益合计(元)", "负债和股东权益合计(元)" ]
    elif part == 'profit':
        header = [ "营业总收入(元)", "营业收入(元)", "金融资产利息收入(元)", "已赚保费(元)", "手续费及佣金收入(元)", "营业总成本(元)",
            "营业成本(元)", "金融资产利息支出(元)", "手续费及佣金支出(元)", "退保金(元)", "赔付支出净额(元)", "提取保险合同准备金净额(元)",
            "保单红利支出(元)", "分保费用(元)", "营业税金及附加(元)", "销售费用(元)", "管理费用(元)", "财务费用(元)", "资产减值损失(元)",
            "公允价值变动收益(元)", "投资收益(元)", "对联营企业和合营企业的投资收益(元)", "汇兑收益(元)", "营业利润(元)", "营业外收入(元)",
            "营业外支出(元)", "非流动资产处置净损失(元)", "利润总额(元)", "所得税(元)", "净利润(元)", "归属于母公司所有者的净利润(元)",
            "少数股东损益(元)", "基本每股收益(元)", "稀释每股收益(元)" ]
    elif part == 'cash':
        header = ["销售商品提供劳务收到的现金(元)", "客户存款和同业存放款项净增加额(元)", "向中央银行借款净增加额(元)",
            "向其他金融机构拆入资金净增加额(元)", "收到原保险合同保费取得的现金(元)", "收到再保险业务现金净额(元)",
            "保户储金及投资款净增加额(元)", "处置交易性金融资产净增加额(元)", "收取利息、手续费及佣金的现金(元)", "拆入资金净增加额(元)",
            "回购业务资金净增加额(元)", "收到的税费返还(元)", "收到的其他与经营活动有关的现金(元)", "经营活动现金流入小计(元)",
            "购买商品接受劳务支付的现金(元)", "客户贷款及垫款净增加额(元)", "存放中央银行和同业款项净增加额(元)",
            "支付原保险合同赔付款项的现金(元)", "支付利息、手续费及佣金的现金(元)", "支付保单红利的现金(元)",
            "支付给职工以及为职工支付的现金(元)", "支付的各项税费(元)", "支付的其他与经营活动有关的现金(元)", "经营活动现金流出小计(元)",
            "经营活动现金流量净额(元)", "收回投资所收到的现金(元)", "取得投资收益所收到的现金(元)",
            "处置固定资产、无形资产和其他长期资产而收回的现金(元)", "收回投资所收到的现金中的出售子公司收到的现金(元)",
            "收到的其他与投资活动有关的现金(元)", "投资活动现金流入小计(元)", "购建固定资产、无形资产和其他长期资产所支付的现金(元)",
            "投资所支付的现金(元)", "质押贷款净增加额(元)", "取得子公司及其他营业单位支付的现金净额(元)",
            "支付的其他与投资活动有关的现金(元)", "投资活动现金流出小计(元)", "投资活动产生的现金流量净额(元)", "吸收投资所收到的现金(元)",
            "吸收投资所收到的现金中的子公司吸收少数股东权益性投资收到的现金(元)", "借款所收到的现金(元)", "发行债券所收到的现金(元)",
            "收到其他与筹资活动有关的现金(元)", "筹资活动现金流入小计(元)", "偿还债务所支付的现金(元)",
            "分配股利利润或偿付利息所支付的现金(元)", "分配股利利润或偿付利息所支付的现金中的支付少数股东的股利(元)",
            "支付的其他与筹资活动有关的现金(元)", "筹资活动现金流出小计(元)", "筹资活动产生的现金流量净额(元)", "汇率变动对现金的影响(元)",
            "现金及现金等价物净增加额(元)", "现金及现金等价物余额(元)" ]
    else:
        raise Exception("invalid parameter 'part'")
    stock_info = data.ix[data["code"]==code, header]
    for i, j in zip(header, stock_info.values.tolist()[0]):
        if isinstance(j, float):
            print(i, "\t", round(j,4))
        else:
            print(i, "\t", j)


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


def draw_plot_of_index(file_dir, start_year, end_year, code, col_name, func=None):
    """
    *  绘制某个指标的历史曲线
    *  @param file_dir 年报数据文件目录 str
    *  @param start_year 起始年份 int
    *  @param end_year 结束年份 int
    *  @param code 股票代码 int
    *  @param col_name 指标列名 str
    *  @param func 特定指标计算函数，必须是index_function中的计算函数 function
    """
    index_points = []
    years = [i for i in range(start_year, end_year+1)]
    for i in years:
        try:
            data = pd.read_csv(file_dir + "announcement_sh_%s.csv" % i, encoding="gbk")
            if func is not None:
                if len(inspect.getargspec(func)[0]) == 1:  # 只需要本年数据的计算公式
                    data[col_name] = func(data)
                elif len(inspect.getargspec(func)[0]) == 2:   # 需要2年数据的计算公式
                    data_b1y = pd.read_csv(file_dir + "announcement_sh_%s.csv" % (i-1), encoding="gbk")
                    data[col_name] = func(data, data_b1y)
            point = data.ix[data["code"]==int(code), col_name].values[0]
            point = float(str(point).replace(",", "")) if point != '-' else 0.0
        except: 
            point = 0.0
        index_points.append(point)
    seaborn.barplot(x=years, y=index_points)
    plt.show()

