#coding:utf-8

from src.python.api.StockData import StockData
import pandas as pd
import requests
import time

class AnnouncementDataDownloader(StockData):
    """
    *  下载xbrl格式的年报数据
    """
    header_companyInfo = [ "公司法定中文名称", "公司法定代表人", "公司注册地址", "公司办公地址邮政编码",
            "公司国际互联网网址", "公司董事会秘书姓名", "公司董事会秘书电话", "公司董事会秘书电子信箱",
            "报告期末股东总数", "每10股送红股数", "每10股派息数（含税）", "每10股转增数", 
            "本期营业收入(元)", "本期营业利润(元)", "利润总额(元)", "归属于上市公司股东的净利润(元)",
            "归属于上市公司股东的扣除非经常性损益的净利润(元)", "经营活动产生的现金流量净额(元)", "总资产(元)", "所有者权益（或股东权益）(元)",
            "基本每股收益(元/股)", "稀释每股收益(元/股)","扣除非经常性损益后的基本每股收益(元/股)", "全面摊薄净资产收益率（%）",
            "加权平均净资产收益率（%）", "扣除非经常性损益后全面摊薄净资产收益率（%）", "扣除非经常性损益后的加权平均净资产收益率（%）",
            "每股经营活动产生的现金流量净额(元/股)", "归属于上市公司股东的每股净资产（元/股）" ]
    header_capital = [ "国家持有的有限售条件流通股份数", "国有法人持有的有限售条件流通股份数", "其他有限售条件的内资流通股份数",
            "境内法人持有的有限售条件流通股份数", "境内自然人持有的有限售条件流通股份数", "有限售条件的外资流通股份数",
            "境外法人持有的有限售条件流通股份数", "境外自然人持有的有限售条件流通股份数", "其他有限售股流通股数",
            "有限售条件流通股数", "无限售条件人民币普通股数", "无限售条件境内上市的外资股数",
            "无限售条件境外上市的外资股数", "其他无限售条件已上市流通股份数", "无限售条件流通股份合计", "股份总数",
            "国家持有的有限售条件流通股份占总股本比例(%)", "国有法人持有的有限售条件流通股份占总股本比例(%)",
            "其他有限售条件的内资流通股份占总股本比例(%)", "境内法人持有的有限售条件流通股份占总股本比例(%)",
            "境内自然人持有的有限售条件流通股份占总股本比例(%)", "有限售条件的外资流通股份占总股本比例(%)",
            "境外法人持有的有限售条件流通股份占总股本比例(%)", "境外自然人持有的有限售条件流通股份占总股本比例(%)",
            "其他有限售股流通股份占总股本比例(%)", "有限售条件流通股占总股本比例(%)", "无限售条件人民币普通股占总股本比例(%)",
            "无限售条件境内上市的外资股占总股本比例(%)", "无限售条件境外上市的外资股占总股本比例(%)",
            "其他无限售条件已上市流通股份占总股本比例(%)", "无限售条件流通股占总股本比例(%)", "股份总数占总股本比例(%)" ]
    header_topTenMap = ["第一大股东", "第二大股东", "第三大股东", "第四大股东", "第五大股东", 
                        "第六大股东", "第七大股东", "第八大股东", "第九大股东", "第十大股东"]
    header_balance = [ "货币资金(元)", "结算备付金(元)", "拆出资金(元)", "交易性金融资产(元)", "应收票据(元)", "应收帐款(元)",
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
    header_profit = [ "营业总收入(元)", "营业收入(元)", "金融资产利息收入(元)", "已赚保费(元)", "手续费及佣金收入(元)", "营业总成本(元)",
            "营业成本(元)", "金融资产利息支出(元)", "手续费及佣金支出(元)", "退保金(元)", "赔付支出净额(元)", "提取保险合同准备金净额(元)",
            "保单红利支出(元)", "分保费用(元)", "营业税金及附加(元)", "销售费用(元)", "管理费用(元)", "财务费用(元)", "资产减值损失(元)",
            "公允价值变动收益(元)", "投资收益(元)", "对联营企业和合营企业的投资收益(元)", "汇兑收益(元)", "营业利润(元)", "营业外收入(元)",
            "营业外支出(元)", "非流动资产处置净损失(元)", "利润总额(元)", "所得税(元)", "净利润(元)", "归属于母公司所有者的净利润(元)",
            "少数股东损益(元)", "基本每股收益(元)", "稀释每股收益(元)" ]
    header_cash = ["销售商品提供劳务收到的现金(元)", "客户存款和同业存放款项净增加额(元)", "向中央银行借款净增加额(元)",
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
    
    def __init__(self):
        super(AnnouncementDataDownloader, self).__init__()
        self.tic = time.time()
    
    def __parse_result_list(self, result_list, result_flag):
        """
        *  从api返回的json中拆分出年报信息
        *  @param result_list: 一个企业的json结果列表 list
        *  @param result_flag: result列表中每一个json结果的指标集名称 list
        *  @return data: 拆分后各年度的指标列表 dict
        """
        data = {i["title"]:{} for i in result_list[0]["columns"][0]}  ##数据字典初始化
        
        for idx, json_flag in enumerate(result_flag):
            result = result_list[idx]
            ## 获取每个field代表的年份
            field_dict = {i["field"]:i["title"] for i in result["columns"][0]}
            ## 获取每个row的值
            idx_row = 0
            for row in result["rows"]:
                for elem in row.items():
                    if elem[0] in field_dict:
                        data[field_dict[elem[0]]][json_flag[1][idx_row]] = elem[1]
                idx_row += 1
        return data
        
    
    def get_announcement_sh_by_code(self, code):
        """
        *  下载上交所一个上市公司的年报数据
        *  @param code: 股票代码
        """
        def get_info(url, data):
            try:
                response = requests.post(url, data).json()
            except Exception as e:
                response = {}
                print(e)
            return response
        
        def check_result_list_valid(result_list):
            return len(result_list) > 0

        data = {"report_year": int(self.year)-1, "stock_id": int(code), "report_period_id": 5000}
        url_companyInfo = "http://listxbrl.sse.com.cn/companyInfo/showmap.do"
        url_capital = "http://listxbrl.sse.com.cn/capital/showmap.do"
        url_topTenMap = "http://listxbrl.sse.com.cn/companyInfo/showTopTenMap.do"
        url_balance = "http://listxbrl.sse.com.cn/companyInfo/showBalance.do"
        url_profit = "http://listxbrl.sse.com.cn/profit/showmap.do"
        url_cash = "http://listxbrl.sse.com.cn/cash/showmap.do"
        
        result_list = []
        result_flag = [("url_companyInfo", self.header_companyInfo), 
                       ("url_capital", self.header_capital), 
                       ("url_topTenMap", self.header_topTenMap),
                       ("url_balance", self.header_balance),
                       ("url_profit", self.header_profit), 
                       ("url_cash", self.header_cash)]
        for i in [url_companyInfo, url_capital, url_topTenMap, url_balance, url_profit, url_cash]:
            response = get_info(i, data)
            if len(response) > 0:    ##post异常的数据不加入result_list中
                result_list.append(response)
        
        if check_result_list_valid(result_list):
            result_data = self.__parse_result_list(result_list, result_flag)
        else: 
            result_data = {}
        return result_data
    
    
    def get_all_announcements_sh(self, save_dir_path):
        """
        *  下载上交所所有上市公司的年报数据
        *  @param save_dir_path 文件存储目录 str
        """
        def get_data(data_dict, columns):
            data = []
            for col in columns:
                if col in data_dict:
                    data.append(data_dict[col])
                else:
                    data.append("")
            return data
        
        stock_list = self.session.execute("select * from stock_list").fetchall()
        stock_code = [int(i[0]) for i in stock_list if i[0].startswith("6")]
        all_columns = self.header_companyInfo + self.header_capital + self.header_topTenMap + \
                  self.header_balance + self.header_profit + self.header_cash
        
        all_data = {}
        for idx, code in enumerate(stock_code):
            print("%s.获取%s的年报信息..." % (idx, str(code)), end=" ")
            try:
                data = self.get_announcement_sh_by_code(code=code)
            except:                  ##忽略只有部分页面的企业(例如600206)
                print("error")
                continue
            if len(data) == 0: continue
            for i in data.items():
                if len(i[1]) == 0: break  ## 忽略空年份(例如601211)
                if i[0] not in all_data:
                    all_data[i[0]] = []
                all_data[i[0]].append([code] + get_data(i[1], all_columns))
            print("用时%ss" % round(time.time()-self.tic,1))
            
        ## 转换成DataFrame
        for i in all_data.items():
            all_data[i[0]] = pd.DataFrame(all_data[i[0]], columns=["code"] + all_columns)
        
        ## 存储到csv中
        for i in all_data.items():
            i[1].to_csv(save_dir_path+"announcement_sh_%s.csv"%i[0], index=False, encoding="gbk")
        print("已将年报信息存储至csv文件中...用时%ss" % round(time.time()-self.tic,1))
            

if __name__ == "__main__":
    save_path = "D:/zzz/App/EclipseWorkspace/stock/data/"
    AnnouncementDataDownloader().get_all_announcements_sh(save_path)

