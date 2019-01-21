#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/29 0029 10:17
# @Author  : Hadrianl 
# @File    : pairTrade_IB


from ib_insync import *

class PairTrader(IB):
    def __init__(self, host, port, clientId=0, timeout=10):
        super(PairTrader, self).__init__()
        self.connect(host, port, clientId=clientId, timeout=timeout)

    def placePairTrade(self, pairInstruments, spread, buysell, vol=1):
        assert buysell in ['BUY', 'SELL']
        ins1, ins2 = pairInstruments
        ticker1 = self.reqMktData(ins1)
        ticker2 = self.reqMktData(ins2)

        # 组合单预处理
        from operator import lt, gt
        comp = lt if buysell == 'BUY' else gt  # 小于价差买进组合，大于价差卖出组合
        if buysell == 'BUY':
            comp = lt
            ins1_direction = 'BUY'
            ins1_price = 'ask'
            ins2_direction = 'SELL'
            ins2_price = 'bid'
        else:
            comp = gt
            ins1_direction = 'SELL'
            ins1_price = 'bid'
            ins2_direction = 'BUY'
            ins2_price = 'ask'

        def arbitrage(ticker):  # 套利下单判断
            price1 = getattr(ticker1, ins1_price)
            price2 = getattr(ticker2, ins2_price)
            current_spread = price1 - price2
            print(current_spread)
            if comp(current_spread, spread):
                ins1_lmt_order = LimitOrder(ins1_direction, vol, price1)
                ins2_lmt_order = LimitOrder(ins2_direction, vol, price2)
                self.placeOrder(ticker1.contract, ins1_lmt_order)
                self.placeOrder(ticker2.contract, ins2_lmt_order)


                ticker1.updateEvent -= arbitrage
                ticker2.updateEvent -= arbitrage

        ticker1.updateEvent += arbitrage
        ticker2.updateEvent += arbitrage

        return arbitrage

    def delPairTrade(self, arbitrage):
        for t in self.tickers():
            if arbitrage in t.updateEvent:
                t.updateEvent -= arbitrage


if __name__ == '__main__':
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=0, timeout=10)

