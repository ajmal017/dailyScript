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
from threading import Lock
import threading
from copy import copy
from queue import Queue, Empty
import logging
import time
import uuid
import datetime as dt
from collections import OrderedDict


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger('CTPTrader')


class Event:
    """
    为松耦合创建的事件类
    """
    __slots__ = ('name', 'slots')

    def __init__(self, name=''):
        self.name = name
        self.slots = []  # list of [obj, weakref, func] sublists

    def connect(self, c, weakRef=True, hiPriority=False):
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
        obj, func = self._split(c)
        for slot in self.slots:
            if (slot[0] is obj or slot[1] and slot[1]() is obj) \
                    and slot[2] is func:
                slot[0] = slot[1] = slot[2] = None
        self.slots = [s for s in self.slots if s != [None, None, None]]
        return self

    def emit(self, *args, **kwargs):
        """ 事件触发"""
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
        清除处理队列
        """
        for slot in self.slots:
            slot[0] = slot[1] = slot[2] = None
        self.slots = []

    @staticmethod
    def init(obj, eventNames):
        """
        初始化事件到对象
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
        划分callable类型
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
    events = ('marketDataUpdateEvent', )

    def __init__(self, broker_id, investor_id, password, request_id=1):
        Event.init(self, Md.events)
        self.logger = logging.getLogger('CTPTrader.Md')
        self.broker_id = broker_id
        self.investor_id = investor_id
        self.password = password
        self.request_id = request_id

        self._lock = Lock()
        self._market_data = {}  # 用于存放订阅的数据


    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id):
        '''
        错误信息记录
        :param info:
        :param request_id:
        :return:
        '''
        if info.ErrorID != 0:
            self.logger.error(f'<ErrorRspInfo>request_id={request_id} ErrorID={info.ErrorID}, ErrorMsg={info.ErrorMsg.decode("gbk")}')
        return info.ErrorID != 0

    def OnFrontConnected(self):
        """
        :return:
        """
        self.logger.info(f'<FrontConnected>-前置机已连接')
        user_login = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                                    UserID=self.investor_id,
                                                    Password=self.password)
        self.ReqUserLogin(user_login, self.request_id)

    def OnFrontDisconnected(self, nReason):

        self.logger.info(f'<FrontDisconnected>-前置机已断开  reason:{nReason}')


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
            self.logger.error(f'<RspUserLogin>登录失败')
            self.ErrorRspInfo(pRspInfo, nRequestID)
        else:
            self.logger.info(f'<RspUserLogin>登录成功{pRspUserLogin}')
            self.logger.info(f'<RspUserLogin>当前交易日为{self.GetTradingDay()}')
            if self._market_data:
                super(MdApiPy, self).SubscribeMarketData(list(self._market_data.keys()))

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            self._market_data.setdefault(pSpecificInstrument.InstrumentID, None)
            self.logger.info(f'<RspSubMarketData>#{pSpecificInstrument.InstrumentID}# 订阅成功!')
        else:
            self.ErrorRspInfo(pRspInfo, nRequestID)

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID == 0:
            try:
                self._market_data.pop(pSpecificInstrument.InstrumentID)
                self.logger.info(f'<RspSubMarketData>#{pSpecificInstrument.InstrumentID}# 取消订阅成功!')
            except KeyError:
                self.logger.error(f'<RspSubMarketData>#{pSpecificInstrument.InstrumentID}# 不存在!')
        else:
            self.ErrorRspInfo(pRspInfo, nRequestID)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        data = copy(pDepthMarketData)
        insID = data.InstrumentID
        if insID in self._market_data:
            self._market_data[insID] = data
            self.marketDataUpdateEvent.emit(data)
        else:
            self.logger.debug(f'<RtnDepthMarketData> not include')

    def SubscribeMarketData(self, pInstrumentID: list):
        ids = [bytes(item, encoding="utf-8") for item in pInstrumentID]
        return super(MdApiPy, self).SubscribeMarketData(ids)

    def UnSubscribeMarketData(self, pInstrumentID: list):
        ids = [bytes(item, encoding="utf-8") for item in pInstrumentID]
        return super(MdApiPy, self).UnSubscribeMarketData(ids)



class Trader(TraderApiPy):
    events = ('userLoginEvent', 'userLogoutEvent','frontConnectEvent', 'frontDisconnectEvent',
              'rtnOrderEvent', 'rtnTradeEvent', 'errorEvent',
              'newOrderEvent', 'openOrderEvent', 'cancelOrderEvent', 'filledEvent',
              'rtnBulletin', 'requestEvent')

    def __init__(self, broker_id, investor_id, password, request_id=1):
        Event.init(self, Trader.events)
        self.logger = logging.getLogger('CTPTrader.Td')
        self.request_id = request_id
        self.broker_id = broker_id.encode()
        self.investor_id = investor_id.encode()
        self.password = password.encode()
        self._orderRef = 0

        self._queue = {}
        self._results = {}
        self._account = None
        self._positions = {'LONG': {}, 'SHORT': {}, 'NET': {}}
        self._positionDetails = {}
        self._trades = {}
        self._orders = {}
        self._instruments = {}
        self._instrumentStatus = {}
        self._commissionRates = {}
        self._marginRates = {}

    def ReqOrderInsert(self, pInputOrder, nRequestID):
        self.newOrderEvent.emit(pInputOrder.to_dict())
        super(Trader, self).ReqOrderInsert(pInputOrder, nRequestID)

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.ErrorRspInfo(pRspInfo, nRequestID)

    def ErrorRspInfo(self, info, request_id=None):
        '''
        错误信息记录
        :param info:
        :param request_id:
        :return:
        '''
        if info.ErrorID != 0:
            self.logger.error(f'<ErrorRspInfo>request_id={request_id} ErrorID={info.ErrorID}, ErrorMsg={info.ErrorMsg.decode("gbk")}')
            self.errorEvent.emit(info)
        return info.ErrorID != 0

    def OnFrontDisconnected(self, nReason):
        self.logger.info(f'<FrontDisconnected>-前置机已断开  reason:{nReason}')
        self.frontDisconnectEvent.emit()

    def OnFrontConnected(self):
        self.logger.info(f'<FrontConnected>-前置机已连接')
        req = ApiStructure.ReqUserLoginField(BrokerID=self.broker_id,
                                             UserID=self.investor_id,
                                             Password=self.password)
        self.ReqUserLogin(req, self.request_id)

        self.frontConnectEvent.emit()

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID != 0:
            self.logger.error(f'<RspUserLogin>登录失败')
            self.ErrorRspInfo(pRspInfo, nRequestID)
        else:
            self.logger.info(f'<RspUserLogin>登录成功{pRspUserLogin}{pRspInfo}')
            self.logger.info(f'<RspUserLogin>当前交易日为{self.GetTradingDay()}')
            self.logger.info(f'<RspUserLogin>MaxOrderRef:{pRspUserLogin.MaxOrderRef}')
            self._orderRef = int(pRspUserLogin.MaxOrderRef)

            inv = ApiStructure.QryInvestorField(BrokerID=self.broker_id, InvestorID=self.investor_id)
            self.ReqQryInvestor(inv, self.inc_request_id())

            req = ApiStructure.SettlementInfoConfirmField(BrokerID=self.broker_id, InvestorID=self.investor_id)
            self.ReqSettlementInfoConfirm(req, self.inc_request_id())

    def _on_login_init(self, delay):

        init_list = [('<on_login_init>发起账户初始化请求',
                      ApiStructure.QryTradingAccountField(BrokerID=self.broker_id, InvestorID=self.investor_id, BizType=' '),
                      self.ReqQryTradingAccount),
                     ('<on_login_init>发起订单信息初始化请求',
                      ApiStructure.QryOrderField(BrokerID=self.broker_id, InvestorID=self.investor_id),
                      self.ReqQryOrder),
                     ('<on_login_init>发起交易信息初始化请求',
                      ApiStructure.QryTradeField(BrokerID=self.broker_id, InvestorID=self.investor_id),
                      self.ReqQryTrade),
                     ('<on_login_init>发起持仓信息初始化请求',
                      ApiStructure.QryInvestorPositionField(BrokerID=self.broker_id, InvestorID=self.investor_id),
                      self.ReqQryInvestorPosition)
                     ]

        for info, qryField, reqFunc in init_list:
            time.sleep(1)  # CTP查询限频
            self.logger.info(info)
            reqFunc(qryField, self.inc_request_id())


    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        self.logger.info(f'<RspSettlementInfoConfirm>结算确认->{pSettlementInfoConfirm.to_dict()}')

    def inc_request_id(self):
        self.request_id += 1
        return self.request_id

    def inc_order_ref(self):
        self._orderRef += 1
        return self._orderRef

    def OnRspQryInvestor(self, pInvestor, pRspInfo, nRequestID, bIsLast):
        self.logger.info(f'<RspQryInvestor>投资者确认->{pInvestor.to_dict()}')
        self.userLoginEvent.emit(5)

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID != 0:
            self.ErrorRspInfo(pRspInfo, nRequestID)
        else:
            self.logger.info(f'<RspOrderInsert>下单响应：{pInputOrder.to_dict()}')

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        if pRspInfo.ErrorID != 0:
            self.ErrorRspInfo(pRspInfo, nRequestID)
        else:
            self.logger.info(f'<RspOrderAction>撤单响应：{pInputOrderAction.to_dict()}')

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        self.logger.error(f'<ErrRtnOrderAction>撤单响应：{pOrderAction.to_dict()}')
        self.ErrorRspInfo(pRspInfo)

    def OnRtnBulletin(self, pBulletin):
        pBulletin_ = copy(pBulletin)
        self.logger.info(f'<RtnBulletin>交易所公告：{pBulletin_.to_dict()}')
        self.rtnBulletin.emit(pBulletin_)

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        self.logger.error(f'<ErrRtnOrderInsert>下单错误回报：{pInputOrder.to_dict()}')
        self.ErrorRspInfo(pRspInfo)

    def OnRtnOrder(self, pOrder):
        pOrder_ = copy(pOrder)
        self.logger.info(f'<RtnOrder>订单回报：{pOrder_}')
        self._orders[b':'.join([pOrder_.ExchangeID, pOrder_.TraderID, pOrder_.OrderLocalID])] = pOrder_

        self.rtnOrderEvent.emit(pOrder_)

        status = pOrder_.OrderStatus

        if status == b'3':
            self.openOrderEvent.emit(pOrder_)
        elif status == b'0':
            self.filledEvent.emit(pOrder_)
        elif status in [b'5', b'2']:
            self.cancelOrderEvent.emit(pOrder_)


    def OnRtnTrade(self, pTrade):
        pTrade_ = copy(pTrade)
        self.logger.info(f'<RtnTrade>成交回报：{pTrade_}')
        self._trades[b':'.join([pTrade_.ExchangeID, pTrade_.TradeID])] = pTrade_

        self.rtnTradeEvent.emit(pTrade_)

        qrypostion = ApiStructure.QryInvestorPositionField(InstrumentID=pTrade_.InstrumentID)
        request_id = self.inc_request_id()
        self.ReqQryInvestorPosition(qrypostion, request_id)

    def OnRspQryExchange(self, pExchange, pRspInfo, nRequestID, bIsLast):
        print('OnRspQryExchange', vars())

    def OnRspQryTradingCode(self, pTradingCode, pRspInfo, nRequestID, bIsLast):
        print('OnRspQryTradingCode', vars())

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        pInstrument_ = copy(pInstrument)
        self.logger.debug(f'<RspQryInstrument>合约查询响应：{pInstrument_}')
        queue = self._queue.get(nRequestID)

        if queue is not None:
            queue.put_nowait(pInstrument_)

        self._instruments[pInstrument_.InstrumentID] = pInstrument_

        if bIsLast:
            self._EndReq(nRequestID)

    def OnRspQryInstrumentCommissionRate(self, pInstrumentCommissionRate, pRspInfo, nRequestID, bIsLast):
        if pInstrumentCommissionRate is not None:
            pInstrumentCommissionRate_ = copy(pInstrumentCommissionRate)
            self.logger.debug(f'<RspQryInstrument>手续费率查询响应：{pInstrumentCommissionRate_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pInstrumentCommissionRate_)

            self._commissionRates[pInstrumentCommissionRate_.InstrumentID] = pInstrumentCommissionRate_

        if bIsLast:
            self._EndReq(nRequestID)

    def OnRspQryInstrumentMarginRate(self, pInstrumentMarginRate, pRspInfo, nRequestID, bIsLast):
        if pInstrumentMarginRate:
            pInstrumentMarginRate_ = copy(pInstrumentMarginRate)
            self.logger.debug(f'<RspQryInstrument>保证金率查询响应：{pInstrumentMarginRate_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pInstrumentMarginRate_)

            self._marginRates[pInstrumentMarginRate_.InstrumentID] = pInstrumentMarginRate_

        if bIsLast:
            self._EndReq(nRequestID)

    def OnRtnInstrumentStatus(self, pInstrumentStatus):  # 更新合约状态
        pIS_ = copy(pInstrumentStatus)
        self.logger.debug(f'<RtnInstrumentStatus>合约{pIS_.InstrumentID}状态于{pIS_.EnterTime}更新为{pIS_.InstrumentStatus}, 原因->{pIS_.EnterReason}')
        self._instrumentStatus[pIS_.InstrumentID] = pIS_

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        if pInvestorPosition is not None:
            pInvestorPosition_ = copy(pInvestorPosition)
            self.logger.debug(f'<RspQryInstrument>持仓查询响应：{pInvestorPosition_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pInvestorPosition_)

            direction = {b'1': 'NET', b'2': 'LONG', b'3': 'SHORT'}[pInvestorPosition.PosiDirection]
            self._positions[direction][pInvestorPosition_.InstrumentID] = pInvestorPosition_

            # if pInvestorPosition_.Position == 0:
            #     self._positions[direction].pop(pInvestorPosition_.InstrumentID)

        if bIsLast:
            self._EndReq(nRequestID)

    def OnRspQryInvestorPositionDetail(self, pInvestorPositionDetail, pRspInfo, nRequestID, bIsLast):
        if pInvestorPositionDetail is not None:
            pInvestorPositionDetail_ = copy(pInvestorPositionDetail)
            self.logger.debug(f'<RspQryInstrument>持仓查询响应：{pInvestorPositionDetail_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pInvestorPositionDetail_)

            self._positionDetails[b':'.join([pInvestorPositionDetail_['ExchangeID'],
                                             pInvestorPositionDetail_['TradeID']])] = pInvestorPositionDetail_


        if bIsLast:
            self._EndReq(nRequestID)

    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        if pOrder is not None:
            pOrder_ = copy(pOrder)
            self.logger.debug(f'<RspQryOrder>订单查询响应：{pOrder_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pOrder_)

            self._orders[b':'.join([pOrder_.ExchangeID, pOrder_.TraderID, pOrder_.OrderLocalID])] = pOrder_

        if bIsLast:
            self._EndReq(nRequestID)

    # 请求查询成交响应
    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        if pTrade is not None:
            pTrade_ = copy(pTrade)
            self.logger.debug(f'<RspQryTrade>成交查询响应：{pTrade_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pTrade_)

            self._trades[b':'.join([pTrade.ExchangeID, pTrade.TradeID])] = pTrade_

        if bIsLast:
            self._EndReq(nRequestID)

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        if pTradingAccount is not None:
            pTradingAccount_ = copy(pTradingAccount)
            self.logger.debug(f'<OnRspQryTradingAccount>交易账户查询响应：{pTradingAccount_}')
            queue = self._queue.get(nRequestID)

            if queue is not None:
                queue.put_nowait(pTradingAccount_)


            self._account = pTradingAccount_


            if bIsLast:
                self._EndReq(nRequestID)

    def OnRspQrySettlementInfo(self, pSettlementInfo, pRspInfo, nRequestID, bIsLast):
        print(vars())

    def OnRtnErrorConditionalOrder(self, pErrorConditionalOrder):
        print('条件单触发后报单的合法性校验错误:', pErrorConditionalOrder)


    def _StartReq(self, nRequestID):
        queue = Queue()
        result = []
        self._queue[nRequestID] = queue
        self._results[nRequestID] = result
        return queue, result

    def _EndReq(self, nRequestID, success=True):
        queue = self._queue.pop(nRequestID, None)

        if queue:
            queue.put_nowait(None)
            result = self._results.pop(nRequestID, [])

    def QryPosition(self, **kwargs):
        qrypostion = ApiStructure.QryInvestorPositionField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryInvestorPosition(qrypostion, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            print('position: Timeout')
            return

    def QryPositionDetail(self, **kwargs):
        qrypostionDetail = ApiStructure.QryInvestorPositionDetailField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryInvestorPosition(qrypostionDetail, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            print('positionDetail: Timeout')
            return

    def QryOrder(self, **kwargs):
        qryorder = ApiStructure.QryOrderField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryOrder(qryorder, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryOrder', 'Timeout')
            return

    def QryTrade(self, **kwargs):
        qrytrade = ApiStructure.QryTradeField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryTrade(qrytrade, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryTrade', 'Timeout')
            return

    def QryInstrument(self, **kwargs):
        qryinstrument = ApiStructure.QryInstrumentField.from_dict(kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryInstrument(qryinstrument, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryInstrument', 'Timeout')
            return

    def QryCommissionRate(self, **kwargs):
        CommissionRate = ApiStructure.QryInstrumentCommissionRateField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryInstrumentCommissionRate(CommissionRate, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryCommissionRate', 'Timeout')
            return

    def QryMarginRate(self, **kwargs):
        kwargs.setdefault('HedgeFlag', b' ')
        MarginRate = ApiStructure.QryInstrumentMarginRateField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryInstrumentMarginRate(MarginRate, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryMarginRate', 'Timeout')
            return

    def QryAccount(self, **kwargs):
        kwargs.setdefault('BizType', ' ')
        qryaccount = ApiStructure.QryTradingAccountField(BrokerID=self.broker_id, InvestorID=self.investor_id, **kwargs)
        request_id = self.inc_request_id()
        queue, result = self._StartReq(request_id)
        self.ReqQryTradingAccount(qryaccount, request_id)
        try:
            while True:
                ret = queue.get(timeout=1)
                if ret:
                    result.append(ret.to_dict())
                else:
                    break
            return result
        except Empty:
            self._QryFailed('QryAccount', 'Timeout')
            return

    def _QryFailed(self, qryType, info):
        self.logger.debug(f'<{qryType}>{info}')


class PairOrders:
    events = ('orderUpdateEvent', 'allFilledEvent',
              'forwardFilledEvent', 'guardFilledEvent',
              'forwardPartlyFilledEvent', 'guardPartlyFilledEvent'
              'finishedEvent')
    def __init__(self, pairInstrumentIDs, spread, buysell, openclose, vol, tolerant_timedelta, orderRefs):
        Event.init(self, PairOrders.events)
        self.id = uuid.uuid1()
        self.pairInstrumentIDs = pairInstrumentIDs
        self.spread = spread
        self.buysell = buysell
        self.openclose = openclose
        self.vol = vol
        self.tolerant_timedelta = dt.timedelta(seconds=tolerant_timedelta)
        self.init_time = None
        self.orders = OrderedDict({k: None for k in orderRefs})

        self._isFinished = False

        self._forwardFilled = False
        self._guardFilled = False
        self._allFilled = False

    def set_init_time(self):
        if self.init_time is None:
            self.init_time = dt.datetime.now()

    def update_order(self, pOrder):
        if pOrder.OrderRef in self.orders:
            self.orders[pOrder.OrderRef] = pOrder  # 更新订单

            if pOrder.OrderStatus == b'0':
                if pOrder.OrderStatus == self.orders.keys()[0]:
                    self._forwardFilled = True
                    self.forwardFilledEvent.emit()
                else:
                    self._guardFilled = True
                    self.guardFilledEvent.emit()

                if self._forwardFilled & self._guardFilled:
                    self.allFilledEvent.emit()
                    self.finishedEvent.emit()

                if self._isFinished:
                    self.finishedEvent.emit()

            elif pOrder.OrderStatus == b'1':
                if pOrder.OrderStatus == self.orders.keys()[0]:
                    self.forwardPartlyFilledEvent.emit()
                else:
                    self.guardPartlyFilledEvent.emit()

    @property
    def filled(self):
        return [o.VolumeTraded for o in self.orders.values()]

    @property
    def total(self):
        return [o.VolumeTotal for o in self.orders.values()]

    @property
    def remaining(self):
        return [o.VolumeTotal - o.VolumeTraded for o in self.orders.values()]

    def isExpired(self):
        return bool(dt.datetime.now() > self.expireTime)

    def isAllFilled(self):
        return self._allFilled

    def isActive(self):
        active = [bool(o in [b'3', b'1']) for o in self.orders.values()]
        return active

    def isFilled(self):
        return self._forwardFilled, self._guardFilled

    def isDone(self):
        return self._isDone


    @property
    def expireTime(self):
        return self.init_time + self.tolerant_timedelta

    def __repr__(self):
        return f'PairOrder: {self.id}'

    def __iter__(self):
        return self.orders.items().__iter__()


class PairTrader():
    def __init__(self, broker_id, investor_id, password, MD_SERVER, TD_SERVER):
        self.md = Md(broker_id, investor_id, password)
        self.td = Trader(broker_id, investor_id, password)
        self.md.Create()
        self.md.RegisterFront(MD_SERVER)
        self.td.Create()
        self.td.RegisterFront(TD_SERVER)
        self.td.SubscribePrivateTopic(1) # 只传送登录后的流内容
        self.td.SubscribePrivateTopic(1) # 只传送登录后的流内容
        self.md.Init()
        self.td.Init()
        self.td._on_login_init(3)
        logger.debug(f'<API>当前API版本{self.md.GetApiVersion()}')

        self._pairOrders_running = []
        self._pairOrders_finished = []

        self.td.rtnOrderEvent += self.update_pairorder

        self.init_unfilled_order_handler()

    @property
    def orders(self):
        data = self.td._orders.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def openOrders(self):
        data = self.td._orders.copy()
        return {k.decode(): v.to_dict() for k, v in data.items() if v.OrderStatus in [b'b', b'a', b'4', b'3', b'1']}

    @property
    def trades(self):
        data = self.td._trades.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def positions(self):
        long = {k.decode(): v.to_dict() for k, v in self.td._positions['LONG'].copy().items() if v.Position != 0}
        short = {k.decode(): v.to_dict() for k, v in self.td._positions['SHORT'].copy().items() if v.Position != 0}
        net = {k.decode(): v.to_dict() for k, v in self.td._positions['NET'].copy().items() if v.Position != 0}
        return {'LONG': long, 'SHORT': short, 'NET': net}

    @property
    def instruments(self):
        data = self.td._instruments.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def instrumentStatus(self):
        data = self.td._instrumentStatus.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def marketData(self):
        data = self.md._market_data.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def commissionRates(self):
        data = self.td._commissionRates.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def marginRates(self):
        data = self.td._marginRates.copy()
        return {k.decode(): v.to_dict() for k, v in data.items()}

    @property
    def account(self):
        data = copy(self.td._account)
        return data.to_dict()


    def placePairTrade(self, pairInstrumentIDs, spread, buysell, openclose, vol=1, tolerant_time_range=30):
        ins1, ins2 = pairInstrumentIDs
        ins1_, ins2_ = ins1.encode(), ins2.encode()
        assert buysell in ['BUY', 'SELL']
        assert openclose in ['OPEN', 'CLOSE', 'CLOSE_TODAY']
        self.md.SubscribeMarketData([ins1, ins2])
        first_request_id = self.td.inc_request_id()
        second_request_id = self.td.inc_request_id()

        # 订单预创建
        from operator import le, ge
        if buysell == 'BUY':  # 买入做多前者做空后者
            comp = le
            first_order = self._init_pair_order(ins1, 'BUY', openclose, vol)
            second_order = self._init_pair_order(ins2, 'SELL', openclose, vol)
            ins1_price_name = 'AskPrice1'
            ins2_price_name = 'BidPrice1'
        else:  # 卖出做空前者做多后者
            comp = ge
            first_order = self._init_pair_order(ins1, 'SELL', openclose, vol)
            second_order = self._init_pair_order(ins2, 'BUY', openclose, vol)
            ins1_price_name = 'BidPrice1'
            ins2_price_name = 'AskPrice1'

        po = PairOrders(pairInstrumentIDs, spread, buysell, openclose, vol, tolerant_time_range, [first_order.OrderRef, second_order.OrderRef])
        self._pairOrders_running.append(po)

        def po_finish():
            po._isFinished = True
            self._pairOrders_finished.append(po)
            self._pairOrders_running.remove(po)

        po.finishedEvent += po_finish

        # 新行情监听函数，价差计算，以及下单条件判断
        def arbitrage(pDepthMarketData):
            logger.debug(f'arbitrage:{pDepthMarketData.InstrumentID}---{pairInstrumentIDs}')
            if pDepthMarketData.InstrumentID not in [ins1_, ins2_]:
                return

            ins1_data = self.md._market_data.get(ins1_, None)
            ins2_data = self.md._market_data.get(ins2_, None)

            if not (ins1_data and ins2_data):
                return


            price1 = getattr(ins1_data, ins1_price_name)
            price2 = getattr(ins2_data, ins2_price_name)
            current_spread = price1 - price2

            # print(buysell, current_spread)
            if comp(current_spread, spread):
                logger.info(f'<arbitrage>{ins1}->{price1} & {ins2}->{price2} 触发{buysell}下单条件:{current_spread}{comp.__qualname__}{spread}')
                first_order.LimitPrice = price1
                second_order.LimitPrice = price2
                self.td.ReqOrderInsert(first_order, first_request_id)
                self.td.ReqOrderInsert(second_order, second_request_id)
                po.set_init_time()  # 初始化挂单时间
                self.md.marketDataUpdateEvent -= arbitrage

        # 把配对下单加入到数据更新事件里
        self.md.marketDataUpdateEvent += arbitrage

        return arbitrage

    def delPairTrade(self, arbitrage):
        self.md.marketDataUpdateEvent -= arbitrage

    def _init_pair_order(self, insID, driection, openclose, vol=1):
        OffsetFlag = {'OPEN': '0', 'CLOSE': '1', 'CLOSE_TODAY': '3'}[openclose]
        order_dict = {'BrokerID': self.td.broker_id,
                      'InvestorID': self.td.investor_id,
                      'InstrumentID': insID,
                      'OrderRef': self.td.inc_order_ref(),
                      'UserID': self.td.investor_id,
                      # 'OrderPriceType': '8' if driection == 'BUY' else 'C',
                      'OrderPriceType': '2',
                      'Direction': '0' if driection == 'BUY' else '1',
                      'CombOffsetFlag': OffsetFlag,
                      'CombHedgeFlag': '1',
                      'LimitPrice': 0,
                      'VolumeTotalOriginal': vol,
                      'TimeCondition': '3',
                      'VolumeCondition': '3',
                      'MinVolume': vol,
                      'ContingentCondition': '1',
                      'StopPrice': 0.0,
                      'ForceCloseReason': '0',
                      # 'RequestID': reqID
                      }

        order = ApiStructure.InputOrderField.from_dict(order_dict)
        return order

    def update_pairorder(self, pOrder):
        for po in self._pairOrders_running:
            po.update_order(pOrder)


    def init_unfilled_order_handler(self):  # 开启报单成交逻辑处理线程
        self._uoh_active = True
        self._unfilled_order_thread = threading.Thread(target=self.unfilled_order_handle)
        self._unfilled_order_thread.start()

    def release_unfilled_order_handler(self):  # 关闭报单成交逻辑处理线程
        self._uoh_active = False
        self._unfilled_order_thread.join()

    def unfilled_order_handle(self):  # 报单成交处理逻辑，***整个交易对保单之后的逻辑都在这里处理
        while True:
            for po in self._pairOrders_running:

                if po.isExpired():
                    is_filled = po.isFilled()
                    if all(not a for a in is_filled):
                        for _, order in po:  # 都未成交，撤单
                            self._del_remain_order(order)
                        continue
                    else:
                        is_close = self._close_pos_profit_order(po)  # FIXME:可以深入优化

                        for i, c in is_close.items():
                            if c == 'pos':  # 判断是否是盈利平仓
                                self._del_remain_order(po.orders.values()[1 - i])  # 已经平仓盈利单的情况下，撤销剩余那一腿
                                po._isFinished = True
                            elif c == 'neg':
                                self._modify_to_op_price(po.orders.values()[1 - i])  # 如果已成交的单未盈利的情况下，追价到最新对手盘最优价
            if self._uoh_active:
                time.sleep(1)  # 每隔一秒执行一次检查
            else:
                break


    def _del_remain_order(self, order):  # 删除未成交仓位
        delOrder = ApiStructure.InputOrderActionField(BrokerID=self.td.broker_id, InvestorID=self.td.investor_id,
                                                      OrderRef=order.OrderRef, FrontID=order.FrontID, SessionID=order.SessionID,  # 三组序列号确定唯一报单
                                                      InstrumentID=order.InstrumentID,
                                                      ActionFlag=b'0')
        self.td.ReqOrderAction(delOrder, self.td.inc_request_id())

    def _close_pos_profit_order(self, pairOrders):  # 平掉获利仓位
        is_close = OrderedDict()
        for i, (ref, order) in enumerate(pairOrders):
            if order.OrderStatus == b'0':
                traded_vol = order.VolumeTraded
                order_dict = {'BrokerID': self.td.broker_id,
                              'InvestorID': self.td.investor_id,
                              'InstrumentID': order.InstrumentID,
                              'OrderRef': self.td.inc_order_ref(),
                              'UserID': self.td.investor_id,
                              'OrderPriceType': '2',
                              'CombHedgeFlag': '1',
                              'TimeCondition': '3',
                              'VolumeCondition': '1',
                              'MinVolume': 0,
                              'ContingentCondition': '1',
                              'StopPrice': 0.0,
                              'ForceCloseReason': '0',
                              # 'RequestID': reqID
                              }
                if order.Direction == b'0':
                    op_price = self.md._market_data[order.InstrumentID].AskPrice1
                    if op_price > order.LimitPrice:
                        order_dict['Direction'] = 1
                        order_dict['CombOffsetFlag'] = '1' if order.CombOffsetFlag == b'0' else '0'
                        order_dict['LimitPrice'] = op_price
                        order_dict['VolumeTotalOriginal'] = traded_vol
                        closeOrder = ApiStructure.InputOrderField.from_dict(order_dict)
                        self.td.ReqOrderInsert(closeOrder, self.td.inc_request_id())
                        is_close[i] = 'pos'
                    else:
                        is_close[i] = 'neg'

                else:
                    op_price = self.md._market_data[order.InstrumentID].BidPrice1
                    if op_price < order.LimitPrice:
                        order_dict['Direction'] = 0
                        order_dict['CombOffsetFlag'] = '1' if order.CombOffsetFlag == b'0' else '0'
                        order_dict['LimitPrice'] = op_price
                        order_dict['VolumeTotalOriginal'] = traded_vol
                        closeOrder = ApiStructure.InputOrderField.from_dict(order_dict)
                        self.td.ReqOrderInsert(closeOrder, self.td.inc_request_id())
                        is_close[i] = 'pos'
                    else:
                        is_close[i] = 'neg'
            else:
                is_close[i] = None


        return is_close

    def _modify_to_op_price(self, order):
        def insert_after_cancel(o):  # 收到订单取消时间后，马上报新单
            if o.OrderRef == order.OrderRef:
                price = getattr(self.md._market_data[order.InstrumentID],
                                'BidPrice1' if order.Direction == b'0' else 'AskPrice1')
                newOrderRef = self.td.inc_order_ref()
                order_dict = {'BrokerID': self.td.broker_id,
                              'InvestorID': self.td.investor_id,
                              'InstrumentID': order.InstrumentID,
                              'OrderRef': newOrderRef,
                              'UserID': self.td.investor_id,
                              'Direction': order.Direction,
                              'CombOffsetFlag': order.CombOffsetFlag,
                              'LimitPrice': price,
                              'VolumeTotalOriginal': order.VolumeTotalOriginal,
                              'OrderPriceType': '2',
                              'CombHedgeFlag': '1',
                              'TimeCondition': '3',
                              'VolumeCondition': '1',
                              'MinVolume': 0,
                              'ContingentCondition': '1',
                              'StopPrice': 0.0,
                              'ForceCloseReason': '0',
                              # 'RequestID': reqID
                              }

                for po in self._pairOrders_running:
                    if o.OrderRef in po.orders:
                        po.orders.pop(o.OrderRef)
                        po.orders.setdefault(str(newOrderRef).encode(), None)
                        break

                newOrder = ApiStructure.InputOrderField.from_dict(order_dict)
                self.td.ReqOrderInsert(newOrder, self.td.inc_request_id())
                self.td.cancelOrderEvent -= insert_after_cancel


        self.td.cancelOrderEvent += insert_after_cancel
        self._del_remain_order(order)



if __name__ == "__main__":
    investor_id = "120324"
    broker_id = "9999"
    password = ""
    md_server = "tcp://180.168.146.187:10010"
    td_server = "tcp://180.168.146.187:10000"

    pair_trader = PairTrader(broker_id, investor_id, password, md_server, td_server)

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

