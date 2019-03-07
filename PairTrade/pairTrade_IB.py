#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/29 0029 10:17
# @Author  : Hadrianl 
# @File    : pairTrade_IB


from ib_insync import *
import logging
import uuid
from collections import OrderedDict, ChainMap
import numpy as np
from KRData.HKData import HKFuture
import datetime as dt
import asyncio
from ibapi.decoder import Decoder
import struct

# logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
util.logToFile('PairTrade.log', logging.WARNING)
logger = logging.getLogger('IBTrader')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)



from typing import List, Dict


def get_ticks(contract, start, end, host='127.0.0.1', port=7497, clientId=16, **kwargs):
    from collections import deque
    from ib_insync import IB, util
    ib = IB()
    ib.connect(host, port, clientId, timeout=10)
    ib.qualifyContracts(contract)
    print(f'{contract}Downloading...... Start:{start} - To: {end}   from:{host}:{port}#{clientId}')
    data = deque()
    last_time = end
    while True:
        ticks = ib.reqHistoricalTicks(contract, '', last_time, 1000, kwargs.get('_type', 'Bid_Ask'),
                                      useRth=kwargs.get('useRth', False), ignoreSize=kwargs.get('ignoreSize', True))
        last_time = ticks[0].time
        print(last_time)
        ticks.reverse()
        data.extendleft(ticks)

        if last_time.astimezone() < start.astimezone():
            while data[0].time.astimezone() < start.astimezone():
                data.popleft()
            break

    ib.disconnect()
    return util.df(data)

class tradeSession:
    class Session(List):
        def __init__(self):
            super().__init__()

        def __str__(self):
            if self.__len__() != 0:
                _from = self.__getitem__(0).time
                _to = self.__getitem__(-1).time
            else:
                _from = ''
                _to = ''
            return f'From: {_from} - To: {_to} <Profit:{self.profit} NetPos:{self.netPos}>'

        def items(self):
            return [{'datetime': f.time ,'price': f.execution.price,
                     'qty': f.execution.shares, 'side': f.execution.side, 'commission': f.commissionReport.commission} for f in self.__iter__()]

        __repr__ = __str__

        @property
        def profit(self):
            profit = 0
            total_qty = 0
            if self.__len__() != 0:
                for f in self.__iter__():
                    qty = f.execution.shares
                    if f.execution.side == 'SLD':
                        profit += f.execution.price * qty * float(f.contract.multiplier) - f.commissionReport.commission
                        total_qty -= qty
                    else:
                        profit -= f.execution.price * qty * float(f.contract.multiplier) - f.commissionReport.commission
                        total_qty += qty

                profit += total_qty * f.execution.price * float(f.contract.multiplier) - f.commissionReport.commission
            return profit

        @property
        def netPos(self):
            pos = sum(f.execution.shares if f.execution.side == 'BOT' else - f.execution.shares for f in self.__iter__())
            return pos

        @property
        def longQty(self):
            qty = sum(f.execution.shares for f in self.__iter__() if f.execution.side == 'BOT')
            return qty

        @property
        def shortQty(self):
            qty = sum(f.execution.shares for f in self.__iter__() if f.execution.side == 'SLD')
            return qty

    def __init__(self):
        self.__trade_data = dict()
        self.__current_session = dict()
        self.__historical_sessions = dict()

    def init(self, fills):
        for f in sorted(fills, key=lambda x: x.time):
            self.addNewFill(f)

    def handleSession(self, fill):
        contract = fill.contract
        cs = self.__current_session.setdefault(contract, self.Session())
        hs = self.__historical_sessions.setdefault(contract, [])
        cs.append(fill)
        if cs.netPos == 0:
            hs.append(cs)
            self.__current_session[contract] = self.Session()

    def addNewFill(self, newFill):
        td = self.__trade_data.setdefault(newFill.contract, [])
        td.append(newFill)
        self.handleSession(newFill)

    @property
    def currentSession(self):
        return self.__current_session

    @property
    def tradeData(self):
        return self.__trade_data

    @property
    def historicalSessions(self):
        return self.__historical_sessions

    def __str__(self):
        text = ""
        text += "HistorySession\n"
        for c in self.__historical_sessions:
            text += f'  {c.localSymbol}:\n'
            for i, sess in enumerate(self.__historical_sessions[c]):
                text += f'      #{i} {sess}\n'

        text += "\nCurrentSession\n"
        for c, sess in self.__current_session.items():
            text += f'  {c.localSymbol}:\n      {sess}\n'

        return text

    def __getitem__(self, item):
        for c in self.__trade_data:
            if item == c.localSymbol:
                return {'historicalSessions': self.__historical_sessions.get(c),
                        'currentSession': self.__current_session.get(c)}
        else:
            raise IndexError

    def __getattr__(self, item):
        for c in self.__trade_data:
            if item == c.localSymbol:
                return {'historicalSessions': self.__historical_sessions.get(c),
                        'currentSession': self.__current_session.get(c)}
        else:
            raise AttributeError

    __repr__ = __str__

class PairOrders:
    events = ('orderUpdateEvent', 'allFilledEvent',
              'forwardFillEvent', 'guardFillEvent',
              'forwardPartlyFilledEvent', 'guardPartlyFilledEvent',
              'finishedEvent', 'initEvent')

    def __init__(self, pairInstruments, spread, buysell, vol, tolerant_timedelta):
        Event.init(self, PairOrders.events)
        self.id = uuid.uuid1()
        self.pairInstruments = pairInstruments
        self.spread = spread
        self.buysell = buysell
        self.vol = vol
        self.tolerant_timedelta = dt.timedelta(seconds=tolerant_timedelta)
        self.init_time = None
        self.init = self.initEvent.wait()
        self.trades = OrderedDict()
        self.extra_trades = OrderedDict()
        self.tickers = {}

        self._order_log = []

        self._isFinished = False

        self._forwardFilled = False
        self._guardFilled = False
        self._allFilled = False

    def __call__(self, *args, **kwargs):
        self._trigger(*args, **kwargs)

    def _trigger(self, *args, **kwargs):
        ...

    def set_init_time(self):
        if self.init_time is None:
            self.init_time = dt.datetime.now()
            trades = list(self.trades.values())
            self.initEvent.emit(self.init_time)

    async def handle_trade(self):
        while True:
            await asyncio.sleep(0.1)
            netPos = 0
            for  key, trade in self.trades.items():
                netPos += trade.filled() * (1 if trade.order.action == 'BUY' else -1)
            else:
                if netPos == 0:
                    return True

    async def status(self):
        return self

    __aiter__ = status

    async def __anext__(self):
        pos = 0
        neg = 0
        total_pnl = 0
        if not self._isFinished:
            for ref, t in ChainMap(self.trades, self.extra_trades).items():
                ticker = self.tickers[t.contract]
                _filled = t.filled()

                if t.order.action == 'BUY':
                    pos += _filled
                    pnl = (ticker.bid - t.order.lmtPrice) * _filled * int(t.contract.multiplier)
                else:
                    neg += _filled
                    pnl = (ticker.ask - t.order.lmtPrice) * _filled * int(t.contract.multiplier)

                net = pos - neg
                total_pnl += pnl

            await asyncio.sleep(1)

            return net, total_pnl
        else:
            raise StopAsyncIteration

    @property
    def filled(self):
        return [t.orderStatus.status == 'Filled' for t in self.trades.values()]

    @property
    def total(self):
        return [t.order.totalQuantity for t in self.trades.values()]

    @property
    def remaining(self):
        return [t.remaining() for t in self.trades.values()]

    def isExpired(self):
        return bool(self.init_time is not None and dt.datetime.now() > self.expireTime)

    def isAllFilled(self):
        return self._allFilled

    def isActive(self):
        return [t.isActive() for t in self.trades.values()]

    def isFilled(self):
        return [self._forwardFilled, self._guardFilled]

    def isFinished(self):
        return self._isFinished

    @property
    def expireTime(self):
        return self.init_time + self.tolerant_timedelta

    def __repr__(self):
        return f'<PairOrder: {self.id}> instrument:{self.pairInstruments} spread:{self.spread} direction:{self.buysell}'

    def __iter__(self):
        return self.orders.items().__iter__()

class PairTrader(IB):
    def __init__(self, host, port, clientId=0, timeout=10):
        super(PairTrader, self).__init__()
        self._pairOrders_running = []  # List:
        self._pairOrders_finished = []
        self._lastUpdateTime = dt.datetime.now()
        # self.updateEvent += self._handle_expired_pairOrders
        self.tradeSession = tradeSession()
        self.execDetailsEvent += self.updateTradeSession
        self.connectedEvent += lambda: self.tradeSession.init(self.fills())  # 用于初始化tradeSession

        self.connect(host, port, clientId=clientId, timeout=timeout)
        self._last_pairOrders = []

    def updateTradeSession(self, trade, fill):
        self.tradeSession.addNewFill(fill)

    # def placeMarketFHedge(self, contracts, action, price, vol):
    #     lmt_order = LimitOrder(action, vol, price)

    def placePairTrade(self, *pairOrderArgs):
        pairTradeAsync = [self.placePairTradeAsync(*args) for args in pairOrderArgs]
        return self._run(*pairTradeAsync)

    async def CalendarSpreadArbitrage(self, pairInstruments, longSpread, ShortSpread, vol=1,
                                      RTH=True):
        insts = self.qualifyContracts(*pairInstruments)
        assert len(insts) == 2, "无法确认合约"

        async def runStrategy():
            self._last_pairOrders = []
            tradeTimeRange = self.initTradeTime(dt.datetime.now(), RTH)
            start = tradeTimeRange[0][0]
            end = tradeTimeRange[-1][1]
            async for t in util.timeRangeAsync(start, end, 1):
                if t > tradeTimeRange[0][1]:
                    tradeTimeRange.pop(0)
                    continue
                elif t < tradeTimeRange[0][0] or t < dt.datetime.now() - dt.timedelta(seconds=1):
                    continue

                to = (tradeTimeRange[0][1] - t).total_seconds()
                logger.info(f'<CalendarSpreadArbitrage>new loop -> {to}seconds to pause the strategy')
                longTask = self.placePairTradeAsync(insts, longSpread, 'BUY', vol)
                shortTask = self.placePairTradeAsync(insts, ShortSpread, 'SELL', vol)
                try:
                    done, pending = await asyncio.wait([longTask, shortTask], timeout=to)
                except TimeoutError:
                    logger.info(f'<CalendarSpreadArbitrage>new loop -> {to}seconds to pause the strategy')
                    # 这里需要做撸平暴露仓位，取消未成交报单的操作
                finally:
                    self._last_pairOrders.extend(done)
            return self._last_pairOrders

        return await runStrategy()


    async def placePairTradeAsync(self, pairInstruments, spread, buysell, vol=1, tolerant_timedelta=90):
        assert buysell in ['BUY', 'SELL']
        tickers = [None, None]
        for i, ins in enumerate(pairInstruments):
            tickers[i] = self.ticker(ins)
            if tickers[i] is None:
                tickers[i] = self.reqMktData(ins)

        po = PairOrders(pairInstruments, spread, buysell, vol, tolerant_timedelta)
        po.tickers = {t.contract: t for t in tickers}

        # 组合单预处理
        from operator import le, ge
        comp = le if buysell == 'BUY' else ge  # 小于价差买进组合，大于价差卖出组合
        if buysell == 'BUY':
            comp = le
            ins1_price = 'ask'
            ins2_price = 'bid'
        else:
            comp = ge
            ins1_price = 'bid'
            ins2_price = 'ask'


        def po_finish():
            if not po._isFinished:
                po._isFinished = True
                self._pairOrders_finished.append(po)
                self._pairOrders_running.remove(po)
                self.pendingTickersEvent -= po

        po.finishedEvent += po_finish  # 主要用于配对交易完成的之后的处理，同running队列删除，移至finished队列。包括的情况有完全成交，单腿成交盈利平仓剩余撤单，全部撤单等情况
        placeFOrder = self.initForwardOrder(po)
        placeGOrder = self.initGuardOrder(po)

        def arbitrage(pendingTickers):  # 套利下单判断
            # if all(ticker not in tickers for ticker in pendingTickers):
            #     logger.warning('<arbitrage>no ticker yet')
            #     return
            price1 = getattr(tickers[0], ins1_price, -1)
            price2 = getattr(tickers[1], ins2_price, -1)
            if price1 == -1 or price2 == -1:
                return
            # current_spread = price1 - price2
            # if comp(current_spread, spread):
            #             #     forward_trade = placeFOrder()
            #             #     if placeGOrder not in forward_trade.filledEvent:
            #             #         forward_trade.filledEvent += placeGOrder

            forward_trade = placeFOrder()
            if placeGOrder not in forward_trade.fillEvent:
                forward_trade.fillEvent += placeGOrder

        po._trigger = arbitrage
        self.pendingTickersEvent += po
        self._pairOrders_running.append(po)
        init_time = await po.init
        logger.info(f'<unfilled_order_handle>{po.id}初始化时间:{init_time}')
        await self.unfilled_order_handle(po)

        return po

    def initForwardOrder(self, pairOrders):
        isFirst = True
        gTicker = pairOrders.tickers[pairOrders.pairInstruments[0]]
        if pairOrders.buysell == 'BUY':
            bs = 'SELL'
            gTicker_type = 'ask'
        else:
            bs = 'BUY'
            gTicker_type = 'bid'

        lmt_order = LimitOrder(bs, pairOrders.vol, -1)
        lmt_order.orderId = self.client.getReqId()
        key = self.wrapper.orderKey(lmt_order.clientId, lmt_order.orderId, lmt_order.permId)
        lmt_order.orderRef = f'pt-Forward->{key}'
        contract = pairOrders.pairInstruments[1]
        def placeForwardOrder():
            nonlocal isFirst
            gTicker_price = getattr(gTicker, gTicker_type, -1)
            forward_trade = pairOrders.trades.get(key)
            price = gTicker_price - pairOrders.spread
            if forward_trade is None or not forward_trade.isDone() and lmt_order.lmtPrice != price:
                lmt_order.lmtPrice = price
                forward_trade = self.placeOrder(contract, lmt_order)

            if isFirst:
                # forward_trade.filledEvent += pairOrders._put_filled
                isFirst = False
                pairOrders.trades[key] = forward_trade


            return forward_trade

        return placeForwardOrder


    def initGuardOrder(self, pairOrders):
        isFirst = True
        gTicker = pairOrders.tickers[pairOrders.pairInstruments[0]]
        if pairOrders.buysell == 'BUY':
            bs = 'BUY'
            gTicker_type = 'ask'
        else:
            bs = 'SELL'
            gTicker_type = 'bid'

        contract = pairOrders.pairInstruments[0]
        def placeGuardOrder(trade, fill):
            nonlocal isFirst
            # lmt_order.lmtPrice = int(trade.orderStatus.avgFillPrice + pairOrders.spread)
            price = getattr(gTicker, gTicker_type, -1)
            lmt_order = LimitOrder(bs, fill.execution.shares, price)
            lmt_order.orderId = self.client.getReqId()
            key = self.wrapper.orderKey(lmt_order.clientId, lmt_order.orderId, lmt_order.permId)
            lmt_order.orderRef = f'pt-Guard->{key}'
            guard_trade = self.placeOrder(contract, lmt_order)
            pairOrders.trades[key] = guard_trade
            if isFirst:
                pairOrders.set_init_time()
                # guard_trade.filledEvent += pairOrders._put_filled
                isFirst = False


        return placeGuardOrder

    # async def placePairTradeAsync(self, pairInstruments, spread, buysell, vol=1, tolerant_timedelta=30):
    #     assert buysell in ['BUY', 'SELL']
    #     # ins1, ins2 = pairInstruments
    #     tickers = [None, None]
    #     for i, ins in enumerate(pairInstruments):
    #         tickers[i] = self.ticker(ins)
    #         if tickers[i] is None:
    #             tickers[i] = self.reqMktData(ins)
    #
    #     po = PairOrders(pairInstruments, spread, buysell, vol, tolerant_timedelta)
    #     po.tickers = {t.contract: t for t in tickers}
    #
    #     # 组合单预处理
    #     from operator import le, ge
    #     comp = le if buysell == 'BUY' else ge  # 小于价差买进组合，大于价差卖出组合
    #     if buysell == 'BUY':
    #         comp = le
    #         ins1_direction = 'BUY'
    #         ins1_price = 'ask'
    #         ins2_direction = 'SELL'
    #         ins2_price = 'bid'
    #     else:
    #         comp = ge
    #         ins1_direction = 'SELL'
    #         ins1_price = 'bid'
    #         ins2_direction = 'BUY'
    #         ins2_price = 'ask'
    #
    #     def po_finish():
    #         if not po._isFinished:
    #             po._isFinished = True
    #             self._pairOrders_finished.append(po)
    #             self._pairOrders_running.remove(po)
    #
    #     po.finishedEvent += po_finish  # 主要用于配对交易完成的之后的处理，同running队列删除，移至finished队列。包括的情况有完全成交，单腿成交盈利平仓剩余撤单，全部撤单等情况
    #
    #     def arbitrage(pendingTickers):  # 套利下单判断
    #         if all(ticker not in tickers for ticker in pendingTickers):
    #             print('no ticker')
    #             return
    #         price1 = getattr(tickers[0], ins1_price)
    #         price2 = getattr(tickers[1], ins2_price)
    #         current_spread = price1 - price2
    #         if comp(current_spread, spread):
    #             ins1_lmt_order = LimitOrder(ins1_direction, vol, price1, orderRef='pt-Forward->[]')
    #             ins2_lmt_order = LimitOrder(ins2_direction, vol, price2, orderRef='pt-Guard->[]')
    #             trade1 = self.placeOrder(tickers[0].contract, ins1_lmt_order)
    #             trade2 = self.placeOrder(tickers[1].contract, ins2_lmt_order)
    #             self.pendingTickersEvent -= po
    #
    #             keys = [self.wrapper.orderKey(o.clientId, o.orderId, o.permId) for o in [ins1_lmt_order, ins2_lmt_order]]
    #             for k, t in zip(keys, [trade1, trade2]):
    #                 po.trades[k] = t
    #             po.set_init_time()
    #
    #
    #
    #     po._trigger = arbitrage
    #     self.pendingTickersEvent += po
    #     self._pairOrders_running.append(po)
    #     init_time = await po.init
    #     logger.info(f'<unfilled_order_handle>{po.id}初始化时间:{init_time}')
    #     await self.unfilled_order_handle(po)
    #
    #     return po

    def delPairTrade(self, pairOrders):
        for t in self.tickers():
            if pairOrders in t.updateEvent:
                t.updateEvent -= pairOrders

    def visualizeSession(self, session, barSize='1 min'):
        from pyecharts import Kline, Page, Line, Overlap, HeatMap, Grid, Bar
        import talib
        from math import ceil
        def kline_tooltip(params):
            # s = params[0].seriesName + '</br>'
            d = params[0].name + '</br>'
            o = '开: ' + params[0].data[1] + '</br>'
            h = '高: ' + params[0].data[4] + '</br>'
            l = '低: ' + params[0].data[3] + '</br>'
            c = '收: ' + params[0].data[2] + '</br>'
            text = d + o + h + l + c
            return text

        def label_tooltip(params):
            text = params.data.coord[2] + '@[' + params.data.coord[1] + ']'
            return text

        trade_lines_raw = []
        pos = 0
        p_pos = 0
        for i in range(len(session)):
            exec1 = session[i].execution
            pos += (exec1.shares if exec1.side == 'BOT' else -exec1.shares)
            for j in range(i + 1, len(session)):
                exec2 = session[j].execution
                if exec1.side != exec2.side:
                    p_pos += (exec2.shares if exec2.side == 'BOT' else -exec2.shares)
                    if abs(pos) > abs(p_pos):
                        continue
                    trade_lines_raw.append([exec1, exec2])
                    p_pos = 0
                    break

        # for i, f in enumerate(session):
        #     exec = f.execution
        #     current_pos = (exec.shares if exec.side == 'BOT' else -exec.shares)
        #     pos += current_pos
        #     trade_lines_raw.append([i, current_pos])
        #     if len(trade_lines_raw) >=2:
        #         net = trade_lines_raw[-1][1] + trade_lines_raw[-2][1]
        #         if net == 0 :
        #             last1 = trade_lines_raw.pop()
        #             last2 = trade_lines_raw.pop()
        #             yield last2, last1
        #         else:
        #             last1 = trade_lines_raw.pop()
        #             last2 = trade_lines_raw[-1]
        #             yield last2.copy(), last1
        #             last2[1] = net


        trade_points = [{'coord': [str(f.time.astimezone().strftime('%Y-%m-%d %H:%M:00')), f.execution.price, f.execution.shares],
                         'name': 'trade',
                         'label': {'show': True, 'formatter': label_tooltip, 'offset': [0, -30], 'fontSize': 15,
                                   'fontFamily': 'monospace', 'color': 'black'},
                         'symbol': 'triangle',
                         'symbolSize': 8 + 3 * f.execution.shares,
                         'symbolKeepAspect': False,
                         'itemStyle': {'color': 'blue' if f.execution.side == 'BOT' else 'green'},
                         'emphasis': {
                             'label': {'show': True, 'formatter': label_tooltip, 'offset': [0, -30], 'fontSize': 30,
                                       'fontFamily': 'monospace', 'color': 'black', 'fontWeight': 'bolder'},
                             'itemStyle': {'color': 'black'}},
                         'symbolRotate': 0 if f.execution.side == 'BOT' else 180} for f in session]

        trade_lines = [[{'coord': [str(exec1.time.astimezone().strftime('%Y-%m-%d %H:%M:00')), exec1.price],
                         'lineStyle': {'type': 'dashed',
                                       'color': 'red' if ((exec2.price - exec1.price) if exec2.side == 'SELL'
                                                          else (exec1.price - exec2.price)) >= 0 else 'green'},

                         'emphasis': {'lineStyle': {'type': 'dashed',
                                                    'color': 'red' if ((
                                                                               exec2.price - exec1.price) if exec2.side == 'SELL'
                                                                       else (
                                                            exec1.price - exec2.price)) >= 0 else 'green',
                                                    'width': 2 + 0.1 * abs(exec2.price - exec1.price) * abs(
                                                        exec1.shares)},
                                      'label': {'show': True,
                                                'formatter': f'{abs(exec2.price - exec1.price)*abs(exec1.shares)}',
                                                'position': 'middle', 'fontSize': 25, }},
                         },
                        {'coord': [str(exec2.time.astimezone().strftime('%Y-%m-%d %H:%M:00')), exec2.price],
                         'lineStyle': {'type': 'dashed',
                                       'color': 'red' if ((exec2.price - exec1.price) if exec2.side == 'SELL'
                                                          else (exec1.price - exec2.price)) >= 0 else 'green', },

                         'emphasis': {'lineStyle': {'type': 'dashed',
                                                    'color': 'red' if ((
                                                                               exec2.price - exec1.price) if exec2.side == 'SELL'
                                                                       else (
                                                            exec1.price - exec2.price)) >= 0 else 'green',
                                                    'width': 2 + 0.1 * abs(exec2.price - exec1.price) * abs(
                                                        exec1.shares)},
                                      'label': {'show': True,
                                                'formatter': f'{abs(exec2.price - exec1.price)*abs(exec1.shares)}',
                                                'position': 'middle', 'fontSize': 25, }
                                      },
                         }] for exec1, exec2 in trade_lines_raw]

        _from = session[0].time - dt.timedelta(minutes=60)
        _to = session[-1].time + dt.timedelta(minutes=60)
        contract = session[0].contract
        total_seconds = int((_to - _from).total_seconds())
        raw_data = self.reqHistoricalData(contract, _to, f'{total_seconds} S' if total_seconds <=86400 else f'{ceil(total_seconds/86400)} D',
                                          barSize, 'TRADES', useRTH=False)

        data = util.df(raw_data)[['date', 'open', 'close', 'low', 'high']]
        _bars = data.values.transpose()
        x_axis = _bars[0].astype(str)

        kline = Kline(f'{contract.localSymbol}-1min KLine    {session}')
        kline.add(
            f'{contract.localSymbol}',
            x_axis,
            _bars[1:].T,
            mark_point_raw=trade_points,
            mark_line_raw=trade_lines,
            # label_formatter=label_tooltip,
            # xaxis_type='category',
            tooltip_trigger='axis',
            tooltip_formatter=kline_tooltip,
            is_datazoom_show=True,
            datazoom_range=[0, 100],
            datazoom_type='horizontal',
            is_more_utils=True)

        overlap_kline = Overlap('KLine', width='1500px', height='600px')
        overlap_kline.add(kline)

        _close = _bars[2].astype(float)
        MA = {}
        for w in [5, 10, 30, 60]:
            try:
                ma = talib.MA(_close, timeperiod=w)
            except Exception as e:
                ...
            else:
                MA[w] = Line(f'MA{w}')
                MA[w].add(f'MA{w}',
                          x_axis,
                          np.round(ma, 2),
                          is_symbol_show=False,
                          is_smooth=True
                          )
                overlap_kline.add(MA[w])

        macdDIFF, macdDEA, macd = talib.MACDEXT(_close, fastperiod=12, fastmatype=1,
                                                slowperiod=26, slowmatype=1,
                                                signalperiod=9, signalmatype=1)
        diff_line = Line('diff')
        dea_line = Line('dea')
        macd_bar = Bar('macd')
        diff_line.add('diff', x_axis, macdDIFF, line_color='yellow', is_symbol_show=False, is_smooth=True)
        dea_line.add('diff', x_axis, macdDEA, line_color='blue', is_symbol_show=False, is_smooth=True)
        macd_bar.add('macd', x_axis, macd, is_visualmap=True, visual_type='color', is_piecewise=True,
                     pieces=[{max: 0}, {min: 0}])

        overlap_macd = Overlap('MACD', width='1500px', height='200px')
        overlap_macd.add(diff_line)
        overlap_macd.add(dea_line)
        overlap_macd.add(macd_bar)

        grid = Grid('Anlysis', width='1500px', height='1000px')
        grid.add(overlap_kline, grid_top='10%', grid_bottom='25%')
        grid.add(overlap_macd, grid_top='80%')

        return grid

    async def unfilled_order_handle(self, pairOrder):  # 报单成交处理逻辑，***整个交易对保单之后的逻辑都在这里处理
        try:
            await asyncio.wait_for(pairOrder.handle_trade(), pairOrder.tolerant_timedelta.total_seconds())
        except asyncio.TimeoutError:
            logger.warning(f'<unfilled_order_handle>{pairOrder}已过期')
            try:
                while pairOrder in self._pairOrders_running:
                    await self._handle_expired_pairOrders(pairOrder)  # FIXME:可以深入优化
            except Exception as e:
                logger.exception(f'<unfilled_order_handle>处理过期配对报单错误')

    async def _handle_expired_pairOrders(self, po):
        async for net, pnl in po:
            if pnl >0:
                logger.warning(
                    f'<_handle_expired_pairOrders>pairOrders:{po.id} 净暴露头寸：{net} 已盈利点数->{pnl}， 已盈利，删除未成交订单，平仓成交订单')
                for key, trade in ChainMap(po.trades, po.extra_trades).items():
                    if trade.isActive():  # 把队列中的报单删除
                        self._close_after_del(trade)
                else:
                    po.finishedEvent.emit()
                    return

            if net == 0:
                logger.warning(
                    f'<_handle_expired_pairOrders>pairOrders:{po.id} 净暴露头寸：{net} 已盈利点数->{pnl}， 撤销未完全成交报单')
                for key, trade in ChainMap(po.trades, po.extra_trades).items():
                    if trade.isActive():  # 把队列中的报单删除
                        self.cancelOrder(trade.order)
                else:
                    po.finishedEvent.emit()

            elif net > 0:
                logger.warning(
                    f'<_handle_expired_pairOrders>pairOrders:{po.id} 净暴露头寸：{net} 理论盈利点数->{pnl}， 撤销所有报单，并平掉暴露仓位')
                for key, trade in ChainMap(po.trades, po.extra_trades).items():
                    if trade.isActive():  # 把队列中的报单删除
                        self._modify_to_op_price(trade, net)

            elif net < 0:
                logger.warning(
                    f'<_handle_expired_pairOrders>pairOrders:{po.id} 净暴露头寸：{net} 理论盈利点数->{pnl}， 撤销所有报单，并平掉暴露仓位')
                for key, trade in ChainMap(po.trades, po.extra_trades).items():
                    if trade.isActive():  # 把队列中的报单删除
                        self._modify_to_op_price(trade, net)


    def _modify_to_op_price(self, trade, net):
        # def insert_after_cancel(t): # 收到订单取消时间后，马上报新单
        if net < 0:
            price = getattr(self.wrapper.tickers[id(trade.contract)], 'ask')
        elif net > 0:
            price = getattr(self.wrapper.tickers[id(trade.contract)], 'bid')

        lmt_order = trade.order
        lmt_order.lmtPrice = price
        new_trade = self.placeOrder(trade.contract, lmt_order)


    def _close_after_del(self, trade):
        def insert_after_cancel(t): # 收到订单取消时间后，马上平仓
            if t.order.action == 'SELL':
                action = 'BUY'
                price = getattr(self.wrapper.tickers[id(t.contract)], 'ask')
            else:
                action = 'SELL'
                price = getattr(self.wrapper.tickers[id(t.contract)], 'bid')

            filled = t.filled()
            if filled > 0:
                lmt_order = LimitOrder(action, t.filled(), price)
                new_trade = self.placeOrder(t.contract, lmt_order)

                for po in self._pairOrders_running:
                    if t in ChainMap(po.trades, po.extra_trades).values():
                        po.extra_trades[self.wrapper.orderKey(t.order.clientId, t.order.orderId, t.order.permId)] = new_trade
                        break

        trade.cancelledEvent += insert_after_cancel
        self.cancelOrder(trade.order)
    @staticmethod
    def initTradeTime(now, OnlyRTH=True):
        today = dt.date.today()
        forenoon = [dt.datetime.combine(today, dt.time(9, 15)), dt.datetime.combine(today, dt.time(11, 59, 59))]
        afternoon = [dt.datetime.combine(today, dt.time(13, 0)), dt.datetime.combine(today, dt.time(15, 59, 59))]
        afterRTH = [dt.datetime.combine(today, dt.time(17, 15)), dt.datetime.combine(today, dt.time(23, 59, 59))]
        tradeTime = [forenoon, afternoon]
        if not OnlyRTH:
            tradeTime.append(afterRTH)
        for td in tradeTime:
            if now > td[1]:
                tradeTime.remove(td)
            elif now > td[0]:
                td[0] = now
        return tradeTime

if __name__ == '__main__':
    ib = IB()
    ib.connect('127.0.0.1', 7497, clientId=0, timeout=10)

