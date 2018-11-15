#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/6 0006 15:53
# @Author  : Hadrianl 
# @File    : tradeRecords_to_DB.py
# @License : (C) Copyright 2013-2017, 凯瑞投资

"""
python tradeRecords_to_DB.py auto_save # 启用成交回报自动保存入库
python tradeRecords_to_DB.py save_all  # 查询成交信息，并将返回保存入库，同时启用成交回报自动保存入库

"""


from pyctp.CTPApi import CTPTrade
import pymysql as pm
from threading import Thread
from pyctp.utils import struct_format, logger
import configparser
import os

conf_path = os.path.dirname(__file__)
conf_parser = configparser.ConfigParser()
conf_parser.read(os.path.join(conf_path, 'conf.ini'))

addr = conf_parser.get('CTPINFO_DEMO', 'addr')
brokerID = conf_parser.get('CTPINFO_DEMO', 'brokerID')
userID = conf_parser.get('CTPINFO_DEMO', 'userID')

db_host = conf_parser.get('MYSQL', 'host')
db_user = conf_parser.get('MYSQL', 'user')
db_password = conf_parser.get('MYSQL', 'password')
db_name = conf_parser.get('MYSQL', 'db')

t_api = CTPTrade()
t_api.connect(userID, input('请输入密码:'), brokerID, addr)
db = pm.connect(db_host, db_user, db_password, db_name, charset='utf8')

# @t_api.register_rtn_callback('OnRtnTrade', log_type='成交入库', log=True)
# def rtn_to_db(trade):
#     trade = struct_format(trade)
#     data = [ ('"' + v + '"' if isinstance(v, str) else str(v))for v in trade.values()]
#     cursor = db.cursor()
#     cursor.execute(f'replace into ctp_trade_records values({",".join(data)})')
#     cursor.close()
#     db.commit()
#
# @t_api.register_rsp_callback('OnRspQryTrade', log_type='成交入库', log=True)
# def rsp_to_db(trade):
#     trade = struct_format(trade)
#     data = [ ('"' + v + '"' if isinstance(v, str) else str(v))for v in trade.values()]
#     cursor = db.cursor()
#     cursor.execute(f'replace into ctp_trade_records values({",".join(data)})')
#     cursor.close()
#     db.commit()

if __name__ == '__main__':
    import sys
    import time
    try:
        arg = sys.argv[1]
    except:
        arg = 'save_all'

    if arg == 'auto_save':
        t_thread = Thread(target=t_api.Join)
        t_thread.start()
    elif arg == 'save_all':
        @t_api.register_rsp_callback('OnRspUserLogin', log_type='登入', log=True)
        def active_qry(pRspUserLogin):
            RspUserLogin = struct_format(pRspUserLogin)
            logger.info(
                f'<登入>交易接口：{RspUserLogin["UserID"]}于{RspUserLogin["LoginTime"]}登录成功,当前交易日为{RspUserLogin["TradingDay"]}')
            t_api.qryTrade()
        t_thread = Thread(target=t_api.Join)
        t_thread.start()
        time.sleep(3)
        t_api.qryTrade()




  # self.BrokerID = '' #经纪公司代码, char[11]
  #       self.InvestorID = '' #投资者代码, char[13]
  #       self.InstrumentID = '' #合约代码, char[31]
  #       self.OrderRef = '' #报单引用, char[13]
  #       self.UserID = '' #用户代码, char[16]
  #       self.ExchangeID = '' #交易所代码, char[9]
  #       self.TradeID = '' #成交编号, char[21]
  #       self.Direction = '' #买卖方向, char
  #       self.OrderSysID = '' #报单编号, char[21]
  #       self.ParticipantID = '' #会员代码, char[11]
  #       self.ClientID = '' #客户代码, char[11]
  #       self.TradingRole = '' #交易角色, char
  #       self.ExchangeInstID = '' #合约在交易所的代码, char[31]
  #       self.OffsetFlag = '' #开平标志, char
  #       self.HedgeFlag = '' #投机套保标志, char
  #       self.Price = '' #价格, double
  #       self.Volume = '' #数量, int
  #       self.TradeDate = 'Date' #成交时期, char[9]
  #       self.TradeTime = 'Time' #成交时间, char[9]
  #       self.TradeType = '' #成交类型, char
  #       self.PriceSource = '' #成交价来源, char
  #       self.TraderID = '' #交易所交易员代码, char[21]
  #       self.OrderLocalID = '' #本地报单编号, char[13]
  #       self.ClearingPartID = 'ParticipantID' #结算会员编号, char[11]
  #       self.BusinessUnit = '' #业务单元, char[21]
  #       self.SequenceNo = '' #序号, int
  #       self.TradingDay = 'Date' #交易日, char[9]
  #       self.SettlementID = '' #结算编号, int
  #       self.BrokerOrderSeq = 'SequenceNo' #经纪公司报单编号, int
  #       self.TradeSource = '' #成交来源, char