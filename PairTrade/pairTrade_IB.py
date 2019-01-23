#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/29 0029 10:17
# @Author  : Hadrianl 
# @File    : pairTrade_IB


from ib_insync import *
import logging
import uuid
from collections import OrderedDict
import datetime as dt
import asyncio

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger('CTPTrader')


class PairOrders:
    events = ('orderUpdateEvent', 'allFilledEvent',
              'forwardFilledEvent', 'guardFilledEvent',
              'forwardPartlyFilledEvent', 'guardPartlyFilledEvent',
              'finishedEvent')
    def __init__(self, pairInstrumentIDs, spread, buysell, vol, tolerant_timedelta, trades):
        Event.init(self, PairOrders.events)
        self.id = uuid.uuid1()
        self.pairInstrumentIDs = pairInstrumentIDs
        self.spread = spread
        self.buysell = buysell
        self.vol = vol
        self.tolerant_timedelta = dt.timedelta(seconds=tolerant_timedelta)
        self.init_time = None
        self.trades = trades

        self._order_log = []

        self._isFinished = False

        self._forwardFilled = False
        self._guardFilled = False
        self._allFilled = False

    def set_init_time(self):
        if self.init_time is None:
            self.init_time = dt.datetime.now()
            self.forwardFilledEvent = self.trades[0].filledEvent
            self.guardFilledEvent = self.trades[1].filledEvent


    # def update_order(self, pOrder):
    #     if pOrder.OrderRef in self.orders:
    #         self.orders[pOrder.OrderRef] = pOrder  # 更新订单
    #         self._order_log.append(pOrder)
    #
    #         if pOrder.OrderStatus == b'0':
    #             if pOrder.OrderRef == list(self.orders.keys())[0]:
    #                 self._forwardFilled = True
    #                 self.forwardFilledEvent.emit()
    #             else:
    #                 self._guardFilled = True
    #                 self.guardFilledEvent.emit()
    #
    #             if self._forwardFilled & self._guardFilled:
    #                 self.allFilledEvent.emit()
    #                 self.finishedEvent.emit()
    #
    #             # if self._isFinished:
    #             #     self.finishedEvent.emit()
    #
    #         elif pOrder.OrderStatus == b'1':
    #             logger.warning(f'订单{pOrder}处于部分成交并在队列中，暂时未对改状态做全面严谨的处理')
    #             if pOrder.OrderRef == list(self.orders.keys())[0]:
    #                 self.forwardPartlyFilledEvent.emit()
    #             else:
    #                 self.guardPartlyFilledEvent.emit()

    @property
    def filled(self):
        return [t.orderStatus.status == 'Filled' for t in self.trades]

    @property
    def total(self):
        return [o.VolumeTotalOriginal for o in self.orders.values()]

    @property
    def remaining(self):
        return [o.VolumeTotal for o in self.orders.values()]

    def isExpired(self):
        return bool(self.init_time is not None and dt.datetime.now() > self.expireTime)

    def isAllFilled(self):
        return self._allFilled

    def isActive(self):
        return [bool(o in [b'3', b'1']) for o in self.orders.values()]

    def isFilled(self):
        return [self._forwardFilled, self._guardFilled]

    def isFinished(self):
        return self._isFinished

    @property
    def expireTime(self):
        return self.init_time + self.tolerant_timedelta

    def __repr__(self):
        return f'<PairOrder: {self.id}> instrument:{self.pairInstrumentIDs} spread:{self.spread} direction:{self.buysell}'

    def __iter__(self):
        return self.orders.items().__iter__()

class PairTrader(IB):
    def __init__(self, host, port, clientId=0, timeout=10):
        super(PairTrader, self).__init__()
        self.connect(host, port, clientId=clientId, timeout=timeout)
        # self.loopUntil()
        # self.waitUntil

        self._pairOrders_running = []
        self._pairOrders_finished = []


    def placePairTrade(self, pairInstruments, spread, buysell, vol=1, tolerant_timedelta=30):
        assert buysell in ['BUY', 'SELL']
        ins1, ins2 = pairInstruments
        ticker1 = self.reqMktData(ins1)
        ticker2 = self.reqMktData(ins2)

        po = PairOrders(pairInstruments, spread, buysell, vol, tolerant_timedelta, [None, None])
        po.tickers = [ticker1, ticker2]

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
                trade1 = self.placeOrder(ticker1.contract, ins1_lmt_order)
                trade2 = self.placeOrder(ticker2.contract, ins2_lmt_order)
                po.trades = [trade1, trade2]
                po.set_init_time()


                ticker1.updateEvent -= po
                ticker2.updateEvent -= po

        po.__call__ = arbitrage
        ticker1.updateEvent += po
        ticker2.updateEvent += po
        self._pairOrders_running.append(po)

        return po

    def delPairTrade(self, pairOrders):
        for t in self.tickers():
            if pairOrders in t.updateEvent:
                t.updateEvent -= pairOrders

    async def _handle_expired_pairOrders(self):
        while True:
            await asyncio.sleep(1)
            async for po in self._pairOrders_running:
                if all(po.isfilled()):
                    self._pairOrders_running.remove(po)
                    self._pairOrders_finished.append(po)
                else:
                    for i, t in enumerate(po.trades):
                        if t.OrderStatus.status == 'Filled':
                            if t.order.action == 'BUY':
                                bid = po.tickers[i].bid
                                order_price = t.order.lmtPrice

                                if bid > order_price:  # 获利平仓并撤另一腿
                                    self.cancelOrder(po.trades[1-i].order)
                                    close_profit_order = LimitOrder('SELL', t.orderStatus.filled, bid)
                                    self.placeOrder(t.contract, close_profit_order)
                                else:  # 否则，则追价未成交的一腿至对价
                                    self.cancelOrder(po.trades[1 - i].order)

                            else:
                                ask = po.tickers[i].ask
                                order_price = t.order.lmtPrice

                                if ask < order_price:  # 获利平仓并撤另一腿
                                    self.cancelOrder(po.trades[1-i].order)
                                    close_profit_order = LimitOrder('BUY', t.orderStatus.filled, ask)
                                    self.placeOrder(t.contract, close_profit_order)
                                else:  # 否则，则追价未成交的一腿至对价
                                    self.cancelOrder(po.trades[1 - i].order)



if __name__ == '__main__':
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=0, timeout=10)

