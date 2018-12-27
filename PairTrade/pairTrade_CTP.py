#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/27 0027 10:04
# @Author  : Hadrianl 
# @File    : pairTrade_CTP


from ctpwrapper import MdApiPy, TraderApiPy
from ctpwrapper import ApiStructure
import sys
import types
import weakref



class Event:
    """
    Enable event passing between loosely coupled components.
    An event contains a list of callables (the listener slots) that are
    called in order when the event is emitted.
    """
    __slots__ = ('name', 'slots')

    def __init__(self, name=''):
        self.name = name
        self.slots = []  # list of [obj, weakref, func] sublists

    def connect(self, c, weakRef=True, hiPriority=False):
        """
        Connect a callable this event.
        The ``+=`` operator can be used as a synonym for this method.
        Args:
            c: The callable to connect.
            weakRef:
                * True: The callable can be garbage collected
                  upon which it will be automatically disconnected from this
                  event
                * False: A strong reference to the callable will be kept
            hiPriority:
                * True: The callable will be placed in the first slot
                * False The callable will be placed last
        """
        if c in self:
            raise ValueError(f'Duplicate callback: {c}')

        obj, func = self._split(c)
        if weakRef and hasattr(obj, '__weakref__'):
            ref = weakref.ref(obj, self._onFinalize)
            obj = None
        else:
            ref = None
        slot = [obj, ref, func]
        if hiPriority:
            self.slots.insert(0, slot)
        else:
            self.slots.append(slot)
        return self

    def disconnect(self, c):
        """
        Disconnect a callable from this event.
        The ``-=`` operator can be used as a synonym for this method.
        Args:
            c: The callable to disconnect. It is valid if the callable is
                already not connected.
        """
        obj, func = self._split(c)
        for slot in self.slots:
            if (slot[0] is obj or slot[1] and slot[1]() is obj) \
                    and slot[2] is func:
                slot[0] = slot[1] = slot[2] = None
        self.slots = [s for s in self.slots if s != [None, None, None]]
        return self

    def emit(self, *args, **kwargs):
        """
        Call all slots in this event with the given arguments.
        """
        for obj, ref, func in self.slots:
            if ref:
                obj = ref()
            if obj is None:
                if func:
                    func(*args, **kwargs)
            else:
                if func:
                    func(obj, *args, **kwargs)
                else:
                    obj(*args, **kwargs)

    def clear(self):
        """
        Clear all slots.
        """
        for slot in self.slots:
            slot[0] = slot[1] = slot[2] = None
        self.slots = []

    @staticmethod
    def init(obj, eventNames):
        """
        Convenience function for initializing events as members
        of the given object.
        """
        for name in eventNames:
            setattr(obj, name, Event(name))

    __iadd__ = connect
    __isub__ = disconnect
    __call__ = emit

    def __repr__(self):
        return f'Event<{self.name}, {self.slots}>'

    def __len__(self):
        return len(self.slots)

    def __contains__(self, c):
        """
        See if callable is already connected.
        """
        obj, func = self._split(c)
        slots = [s for s in self.slots if s[2] is func]
        if obj is None:
            funcs = [s[2] for s in slots if s[0] is None and s[1] is None]
            return func in funcs
        else:
            objIds = set(id(s[0]) for s in slots if s[0] is not None)
            refdIds = set(id(s[1]()) for s in slots if s[1])
            return id(obj) in objIds | refdIds

    def _split(self, c):
        """
        Split given callable in (object, function) tuple.
        """
        if isinstance(c, types.FunctionType):
            t = (None, c)
        elif isinstance(c, types.MethodType):
            t = (c.__self__, c.__func__)
        elif isinstance(c, types.BuiltinMethodType):
            if type(c.__self__) is type:
                # built-in method
                t = (c.__self__, c)
            else:
                # built-in function
                t = (None, c)
        elif hasattr(c, '__call__'):
            t = (c, None)
        else:
            raise ValueError(f'Invalid callable: {c}')
        return t

    def _onFinalize(self, ref):
        for slot in self.slots:
            if slot[1] is ref:
                slot[0] = slot[1] = slot[2] = None
        self.slots = [s for s in self.slots if s != [None, None, None]]




class Md(MdApiPy):
    """
    """
    events = ('marketDataUpdateEvent')

    def __init__(self, broker_id, investor_id, password, request_id=1):
        Event.init(self, Md.events)
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self.request_id = request_id

        self.market_data = {}  # 用于存放订阅的数据


    def OnRspError(self, pRspInfo, nRequestID, bIsLast):

        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        """
        :param info:
        :return:
        """
        if info.ErrorID != 0:
            print('request_id=%s ErrorID=%d, ErrorMsg=%s',
                  request_id, info.ErrorID, info.ErrorMsg.decode('gbk'))
        return info.ErrorID != 0

    def OnFrontConnected(self):
        """
        :return:
        """

        user_login = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                                    UserID=self.investor_id,
                                                    Password=self.password)
        self.ReqUserLogin(user_login, self.request_id)

    def OnFrontDisconnected(self, nReason):

        print("Md OnFrontDisconnected %s", nReason)
        # sys.exit()

    def OnHeartBeatWarning(self, nTimeLapse):
        """心跳超时警告。当长时间未收到报文时，该方法被调用。
        @param nTimeLapse 距离上次接收报文的时间
        """
        print('Md OnHeartBeatWarning, time = %s', nTimeLapse)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """
        用户登录应答
        :param pRspUserLogin:
        :param pRspInfo:
        :param nRequestID:
        :param bIsLast:
        :return:
        """
        if pRspInfo.ErrorID != 0:
            print("Md OnRspUserLogin failed error_id=%s msg:%s",
                  pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk'))
        else:
            print("Md user login successfully")
            print(pRspUserLogin)
            print(pRspInfo)

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            self.market_data.setdefault(pSpecificInstrument.InstrumentID, None)
            print(f'<{pSpecificInstrument.InstrumentID}>SubMarketData Succeed!')
        else:
            print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            try:
                self.market_data.pop(pSpecificInstrument.InstrumentID)
                print(f'<{pSpecificInstrument.InstrumentID}>UnSubMarketData Succeed!')
            except KeyError:
                print(f'<{pSpecificInstrument.InstrumentID}> Not Exist!')
        else:
            print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        if pDepthMarketData.InstrumentID in self.market_data:
            self.market_data[pDepthMarketData.InstrumentID] = pDepthMarketData
            self.marketDataUpdateEvent.emit(pDepthMarketData)
        else:
            print(f'<pDepthMarketData.InstrumentID> not include')

    def OnRspSubForQuoteRsp(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def  OnRspUnSubForQuoteRsp(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        print(pSpecificInstrument, pRspInfo, nRequestID, bIsLast)

    def OnRtnForQuoteRsp(self, pForQuoteRsp):
        print(pForQuoteRsp)

    def SubscribeMarketData(self, pInstrumentID: list):
        ids = [bytes(item, encoding="utf-8") for item in pInstrumentID]
        return super(MdApiPy, self).SubscribeMarketData(ids)


# BORDKER_ID = ""
# INVESTOR_ID = ""
# PASSWORD = ""
# SERVER = ""
#
# md = Md(BORDKER_ID, INVESTOR_ID, PASSWORD)
# md.Create()
# md.RegisterFront(SERVER)
# md.Init()
# day = md.GetTradingDay()
#
# print(day)
# print("api worker!")


class Trader(TraderApiPy):

    def __init__(self, broker_id, investor_id, password, request_id=1):
        self.request_id = request_id
        self.broker_id = broker_id.encode()
        self.investor_id = investor_id.encode()
        self.password = password.encode()

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):

        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        """
        :param info:
        :return:
        """
        if info.ErrorID != 0:
            print('request_id=%s ErrorID=%d, ErrorMsg=%s',
                  request_id, info.ErrorID, info.ErrorMsg.decode('gbk'))
        return info.ErrorID != 0

    def OnHeartBeatWarning(self, nTimeLapse):
        """心跳超时警告。当长时间未收到报文时，该方法被调用。
        @param nTimeLapse 距离上次接收报文的时间
        """
        print("on OnHeartBeatWarning time: ", nTimeLapse)

    def OnFrontDisconnected(self, nReason):
        print("on FrontDisConnected disconnected", nReason)

    def OnFrontConnected(self):

        req = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                             UserID=self.investor_id,
                                             Password=self.password)
        self.ReqUserLogin(req, self.request_id)
        print("trader on front connection")

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):

        if pRspInfo.ErrorID != 0:
            print("Md OnRspUserLogin failed error_id=%s msg:%s",
                  pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk'))
        else:
            print("Md user login successfully")

            inv = ApiStructure.QryInvestorField(BrokerID=self.broker_id, InvestorID=self.investor_id)

            self.ReqQryInvestor(inv, self.inc_request_id())
            req = ApiStructure.SettlementInfoConfirmField.from_dict({"BrokerID": self.broker_id,
                                                                     "InvestorID": self.investor_id})

            self.ReqSettlementInfoConfirm(req, self.inc_request_id())

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print(pSettlementInfoConfirm, pRspInfo)
        print(pRspInfo.ErrorMsg.decode("GBK"))

    def inc_request_id(self):
        self.request_id += 1
        return self.request_id

    def OnRspQryInvestor(self, pInvestor, pRspInfo, nRequestID, bIsLast):
        print(pInvestor, pRspInfo)


class PairTrade():
    def __init__(self, broker_id, investor_id, password, MD_SERVER, TD_SERVER):
        self.md = Md(broker_id, investor_id, password)
        self.td = Trader(broker_id, investor_id, password)
        self.md.Create()
        self.md.RegisterFront(MD_SERVER)
        self.td.Create()
        self.td.RegisterFront(TD_SERVER)
        self.td.SubscribePrivateTopic(2) # 只传送登录后的流内容
        self.td.SubscribePrivateTopic(2) # 只传送登录后的流内容
        self.md.Init()
        print(f'当前API版本{self.md.GetApiVersion()}')
        print(f'当前MD交易日：{self.md.GetTradingDay()}')
        self.td.Init()
        print(f'当前TD交易日：{self.td.GetTradingDay()}')
        print('初始化完成')

        self.market_data = self.md.market_data

    def addPairTrade(self, pairInstrumentIDs, spread, buysell, openclose, vol=1):
        ins1, ins2 = pairInstrumentIDs
        assert buysell in ['BUY', 'SELL']
        assert openclose in ['OPEN', 'CLOSE', 'CLOSE_TODAY']
        self.md.SubscribeMarketData([ins1, ins2])
        first_request_id = self.inc_request_id()
        second_request_id = self.inc_request_id()

        from operator import lt, gt
        if buysell == 'BUY':  # 买入做多前者做空后者
            comp = lt
            first_order = self._init_pair_order(ins1, 'BUY', openclose, vol, ref=f'pair(<{first_request_id}>, {second_request_id})')
            second_order = self._init_pair_order(ins2, 'SELL', openclose, vol, ref=f'pair({first_request_id}, <{second_request_id}>)')
        else:  # 卖出做空前者做多后者
            comp = gt
            first_order = self._init_pair_order(ins1, 'SELL', openclose, vol,
                                                ref=f'pair(<{first_request_id}>, {second_request_id})')
            second_order = self._init_pair_order(ins2, 'BUY', openclose, vol,
                                                 ref=f'pair({first_request_id}, <{second_request_id}>)')

        # while True:
            # if comp is lt:
            #     first_order.LimitPrice = self.market_data[ins1].AskPrice1
            #     second_order.LimitPrice = self.market_data[ins2].BidPrice1
            # else:
            #     first_order.LimitPrice = self.market_data[ins1].BidPrice1
            #     second_order.LimitPrice = self.market_data[ins2].AskPrice1
        def arbitrage(pDepthMarketData):
            if pDepthMarketData.InstrumentID not in pairInstrumentIDs:
                return

            if comp(self.market_data[ins1].LastPrice - self.market_data[ins2].LastPrice,  spread):
                self.td.ReqOrderInsert(first_order, first_request_id)
                self.td.ReqOrderInsert(second_order, second_request_id)
                self.md.marketDataUpdateEvent -= arbitrage

        self.md.marketDataUpdateEvent += arbitrage

        return arbitrage

    def delPairTrade(self, arbitrage):
        self.md.marketDataUpdateEvent -= arbitrage

    def _init_pair_order(self, insID, driection, openclose, vol=1, ref=''):
        OffsetFlag = {'OPEN': '0', 'CLOSE': '1', 'CLOSE_TODAY': '3'}[openclose]
        order_dict = {'BrokerID': self.td.broker_id,
                      'InvestorID': self.td.investor_id,
                      'InstrumentID': insID,
                      'OrderRef': ref,
                      'UserID': self.td.investor_id,
                      'OrderPriceType': '8' if driection == 'BUY' else 'C',
                      'Direction': '0' if driection == 'BUY' else '1',
                      'CombOffsetFlag': OffsetFlag,
                      'CombHedgeFlag': '2',
                      'LimitPrice': 0.0,
                      'VolumeTotalOriginal': vol,
                      'TimeCondition': '5',
                      'VolumeCondition': '1',
                      'MinVolume': 0,
                      'ContingentCondition': '1',
                      'StopPrice': 0.0,
                      'ForceCloseReason': '0',
                      }

        order = ApiStructure.InputOrderField.from_dict(order_dict)
        return order



if __name__ == "__main__":
    investor_id = "120324"
    broker_id = "9999"
    password = "127565568yjd"
    md_server = "tcp://180.168.146.187:10010"
    td_server = "tcp://180.168.146.187:10000"

    pair_trader = PairTrade(broker_id, investor_id, password, md_server, td_server)

    # user_trader = Trader(broker_id=broker_id, investor_id=investor_id, password=password)
    #
    # user_trader.Create()
    # user_trader.RegisterFront(server)
    # user_trader.SubscribePrivateTopic(2) # 只传送登录后的流内容
    # user_trader.SubscribePrivateTopic(2) # 只传送登录后的流内容
    #
    # user_trader.Init()
    #
    # print("trader started")
    # print(user_trader.GetTradingDay())
    #
    # user_trader.Join()