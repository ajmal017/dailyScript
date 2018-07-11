#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/7/9 0009 14:02
# @Author  : Hadrianl 
# @File    : SP_tradeRecords_to_DB.py.py
# @License : (C) Copyright 2013-2017, 凯瑞投资

import pymysql as pm
from spapi.spAPI import *

def save_trade(trade):
    trade_dict = dict()
    try:
        for n, t in trade._fields_:
            v = getattr(trade, n)
            trade_dict[n] = v.decode() if isinstance(v, bytes) else v
        cursor = conn.cursor()

        values = ','.join(['"' + str(v) + '"' for v in trade_dict.values()])
        sql = f'replace into sp_trade_records values({values})'
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        print(f'插入数据库:{trade_dict}')
    except Exception as e:
        print(e)



if __name__ == '__main__':
    import configparser
    import os
    conf_path = os.path.dirname(__file__)
    conf_parser = configparser.ConfigParser()
    conf_parser.read(os.path.join(conf_path, 'conf.ini'))
    host = conf_parser.get('SPINFO', 'host')
    port = conf_parser.getint('SPINFO', 'port')
    License = conf_parser.get('SPINFO', 'license')
    app_id = conf_parser.get('SPINFO', 'appid')
    user_id = conf_parser.get('SPINFO', 'userid')

    db_host = conf_parser.get('MYSQL', 'host')
    db_user = conf_parser.get('MYSQL', 'user')
    db_password = conf_parser.get('MYSQL', 'password')
    db_name = conf_parser.get('MYSQL', 'db')
    initialize()
    print('无任何消息输出时按任意键退出.....')
    info = {'host': host, 'port': int(port), 'License': License, 'app_id': app_id, 'user_id': user_id,
            'password': input('请输入密码：')}
    set_login_info(**info)
    login()
    conn = pm.connect(host=db_host, port=3306, user=db_user, passwd=db_password,
                      db=db_name)
    conn.set_charset('utf8')


    @on_login_reply  # 登录调用
    def login_reply(user_id, ret_code, ret_msg):
        if ret_code == 0:
            print(f'<账户>{user_id.decode()}登录成功')
        else:
            print(f'<账户>{user_id.decode()}登录失败--errcode:{ret_code}--errmsg:{ret_msg.decode()}')


    @on_load_trade_ready_push  # 登入后，登入前已存的成交信息推送
    def trade_ready_push(rec_no, trade):
        print('<成交>', f'历史成交记录--NO:{rec_no}--{trade.OpenClose.decode()}成交@{trade.ProdCode.decode()}--{trade.BuySell.decode()}--Price:{trade.AvgPrice}--Qty:{trade.Qty}')
        save_trade(trade)

    input()
    # trades = get_all_trades_by_array()
    # for t in trades:
    #     save_trade(t)
    conn.close()
    logout()