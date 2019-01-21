#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/1/2 0002 12:39
# @Author  : Hadrianl 
# @File    : trade_saver


from ib_insync import *
import pymongo as pm

def main(port):
    ib = IB()
    client = pm.MongoClient('192.168.2.226',27017)
    auth_db = client.get_database('admin')
    auth_db.authenticate('KRdata', '')
    db = client.get_database('IB')
    col = db.get_collection('Trade')
    col.create_index([('time', pm.DESCENDING), ('contract.tradingClass', pm.DESCENDING)])
    col.create_index([('execution.execId', pm.DESCENDING)], unique=True)

    def save_fill(fill):
        f = {'time': fill.time, 'contract': fill.contract.dict(), 'execution': fill.execution.dict(),
             'commissionReport': fill.commissionReport.dict()}
        print('<入库>成交记录:', f)
        col.replace_one({'execution.execId': f['execution']['execId']}, f, upsert=True)

    def save_trade(trade, fill):
        print('<执行更新>')
        if trade.orderStatus.status == 'Filled':
            # t = trade.dict()
            # t['contract'] = t['contract'].dict()
            # t['order'] = t['order'].dict()
            # t['orderStatus'] = t['orderStatus'].dict()
            # t['fills'] = [{'time': f.time, 'contract': f.contract.dict(), 'execution': f.execution.dict(),
            #                'commissionReport': f.commissionReport.dict()} for f in t['fills']]
            # t['log'] = [{'time': l.time, 'status': l.status, 'message': l.message} for l in t['log']]
            save_fill(fill)


    ib.execDetailsEvent += save_trade
    ib.connect('192.168.2.117', port, clientId=0, timeout=20)
    print('connected')
    fills = ib.fills()
    for f in fills:
        save_fill(f)
    IB.run()

if __name__ == '__main__':
    main(7496)
    from ibapi.common import HistogramData