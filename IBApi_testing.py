#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/12/6 0006 13:27
# @Author  : Hadrianl 
# @File    : IBApi_testing


import argparse
import datetime
import collections
import inspect

import logging
import time
import os.path

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper

# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue
import queue
from ibapi.utils import (current_fn_name, BadMessage)
from ibapi.errors import *
from ibapi.account_summary_tags import *

from .ContractSamples import ContractSamples
from .OrderSamples import OrderSamples
from .AvailableAlgoParams import AvailableAlgoParams
from .ScannerSubscriptionSamples import ScannerSubscriptionSamples
from .FaAllocationSamples import FaAllocationSamples
from ibapi.scanner import ScanData


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
                        filemode="w",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)



class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
    #     self.setupDetectReqId()
    #
    # def countReqId(self, methName, fn):
    #     def countReqId_(*args, **kwargs):
    #         self.clntMeth2callCount[methName] += 1
    #         idx = self.clntMeth2reqIdIdx[methName]
    #         if idx >= 0:
    #             sign = -1 if 'cancel' in methName else 1
    #             self.reqId2nReq[sign * args[idx]] += 1
    #         return fn(*args, **kwargs)
    #
    #     return countReqId_
    #
    # def setupDetectReqId(self):
    #
    #     methods = inspect.getmembers(EClient, inspect.isfunction)
    #     for (methName, meth) in methods:
    #         if methName != "send_msg":
    #             # don't screw up the nice automated logging in the send_msg()
    #             self.clntMeth2callCount[methName] = 0
    #             # logging.debug("meth %s", name)
    #             sig = inspect.signature(meth)
    #             for (idx, pnameNparam) in enumerate(sig.parameters.items()):
    #                 (paramName, param) = pnameNparam # @UnusedVariable
    #                 if paramName == "reqId":
    #                     self.clntMeth2reqIdIdx[methName] = idx
    #
    #             setattr(TestClient, methName, self.countReqId(methName, meth))

    def run(self):
        try:
            while not self.done and (self.isConnected()
                        or not self.msg_queue.empty()):
                try:
                    try:
                        text = self.msg_queue.get(block=True, timeout=0.2)
                        if len(text) > MAX_MSG_LEN:
                            self.wrapper.error(NO_VALID_ID, BAD_LENGTH.code(),
                                "%s:%d:%s" % (BAD_LENGTH.msg(), len(text), text))
                            self.disconnect()
                            break
                    except queue.Empty:
                        logging.debug("queue.get: empty")
                    else:
                        fields = comm.read_fields(text)
                        logging.debug("fields %s", fields)
                        self.decoder.interpret(fields)
                except (KeyboardInterrupt, SystemExit):
                    logging.info("detected KeyboardInterrupt, SystemExit")
                    self.keyboardInterrupt()
                    self.keyboardInterruptHard()
                except BadMessage:
                    logging.info("BadMessage")
                    self.conn.disconnect()

                logging.debug("conn:%d queue.sz:%d",
                             self.isConnected(),
                             self.msg_queue.qsize())
        finally:
            self.disconnect()


class TestWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
    #     self.setupDetectWrapperReqId()
    #
    # # TODO: see how to factor this out !!
    #
    # def countWrapReqId(self, methName, fn):
    #     def countWrapReqId_(*args, **kwargs):
    #         self.wrapMeth2callCount[methName] += 1
    #         idx = self.wrapMeth2reqIdIdx[methName]
    #         if idx >= 0:
    #             self.reqId2nAns[args[idx]] += 1
    #         return fn(*args, **kwargs)
    #
    #     return countWrapReqId_
    #
    # def setupDetectWrapperReqId(self):
    #
    #     methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
    #     for (methName, meth) in methods:
    #         self.wrapMeth2callCount[methName] = 0
    #         # logging.debug("meth %s", name)
    #         sig = inspect.signature(meth)
    #         for (idx, pnameNparam) in enumerate(sig.parameters.items()):
    #             (paramName, param) = pnameNparam # @UnusedVariable
    #             # we want to count the errors as 'error' not 'answer'
    #             if 'error' not in methName and paramName == "reqId":
    #                 self.wrapMeth2reqIdIdx[methName] = idx
    #
    #         setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))

    @iswrapper
    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        print("RealTimeBar. TickerId:", reqId, RealTimeBar(time, -1, open_, high, low, close, volume, wap, count))

    @iswrapper
    # ! [updatemktdepth]
    def updateMktDepth(self, reqId: TickerId, position: int, operation: int,
                       side: int, price: float, size: int):
        super().updateMktDepth(reqId, position, operation, side, price, size)
        print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:",
              operation, "Side:", side, "Price:", price, "Size:", size)
    # ! [updatemktdepth]

    @iswrapper
    def orderStatus(self, orderId:OrderId , status:str, filled:float,
                    remaining:float, avgFillPrice:float, permId:int,
                    parentId:int, lastFillPrice:float, clientId:int,
                    whyHeld:str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
              "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
              "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
              lastFillPrice, "ClientId:", clientId, "WhyHeld:",
              whyHeld, "MktCapPrice:", mktCapPrice)

    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. ID:", orderId, "Symbol:", contract.symbol, "SecType:", contract.secType,
              "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
              "TotalQuantity:", order.totalQuantity, "Status:", orderState.status)

        if order.whatIf and orderState is not None:
            print("WhatIf. OrderId: ", orderId, "initMarginBefore:", orderState.initMarginBefore, "maintMarginBefore:", orderState.maintMarginBefore,
             "equityWithLoanBefore:", orderState.equityWithLoanBefore, "initMarginChange:", orderState.initMarginChange, "maintMarginChange:", orderState.maintMarginChange,
             "equityWithLoanChange:", orderState.equityWithLoanChange, "initMarginAfter:", orderState.initMarginAfter, "maintMarginAfter:", orderState.maintMarginAfter,
             "equityWithLoanAfter:", orderState.equityWithLoanAfter)

        order.contract = contract
        self.permId2ord[order.permId] = order

    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        print("OpenOrderEnd")

        logging.debug("Received %d openOrders", len(self.permId2ord))
    # ! [openorderend]

    @iswrapper
    # ! [position]
    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        print("Position.", "Account:", account, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency,
              "Position:", position, "Avg cost:", avgCost)
    # ! [position]

    @iswrapper
    # ! [positionend]
    def positionEnd(self):
        super().positionEnd()
        print("PositionEnd")
    # ! [positionend]

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    def test_order(self):
        lmt = OrderSamples.LimitOrder("BUY", 100, 20)
        # The active order will be cancelled if conditioning criteria is met
        lmt.conditionsCancelOrder = True
        lmt.conditions.append(
            OrderSamples.PriceCondition(PriceCondition.TriggerMethodEnum.Last,
                                        208813720, "SMART", 600, False, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), lmt)


class KRApp(TestWrapper, TestClient):
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None

    # def dumpTestCoverageSituation(self):
    #     for clntMeth in sorted(self.clntMeth2callCount.keys()):
    #         logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
    #                                                self.clntMeth2callCount[clntMeth]))
    #
    #     for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
    #         logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
    #                                                self.wrapMeth2callCount[wrapMeth]))
    #
    # def dumpReqAnsErrSituation(self):
    #     logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
    #     for reqId in sorted(self.reqId2nReq.keys()):
    #         nReq = self.reqId2nReq.get(reqId, 0)
    #         nAns = self.reqId2nAns.get(reqId, 0)
    #         nErr = self.reqId2nErr.get(reqId, 0)
    #         logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        print("NextValidId:", orderId)
    # ! [nextvalidid]

        # we can start now
        self.start()

    def start(self):
        if self.started:
            return

        self.started = True

        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            print("Executing requests")
            # self.reqGlobalCancel()
            # self.marketDataTypeOperations()
            # self.accountOperations_req()
            # self.tickDataOperations_req()
            # self.marketDepthOperations_req()
            self.realTimeBarsOperations_req()
            # self.historicalDataOperations_req()
            # self.optionsOperations_req()
            # self.marketScannersOperations_req()
            # self.fundamentalsOperations_req()
            # self.bulletinsOperations_req()
            # self.contractOperations()
            # self.newsOperations_req()
            # self.miscelaneousOperations()
            # self.linkingOperations()
            # self.financialAdvisorOperations()
            # self.orderOperations_req()
            # self.rerouteCFDOperations()
            # self.marketRuleOperations()
            # self.pnlOperations_req()
            # self.histogramOperations_req()
            # self.continuousFuturesOperations_req()
            # self.historicalTicksOperations()
            # self.tickByTickOperations_req()
            # self.whatIfOrderOperations()

            print("Executing requests ... finished")

    def realTimeBarsOperations_req(self):
        self.reqRealTimeBars(3001, ContractSamples.EurGbpFx(), 5, "MIDPOINT", True, [])


    def realTimeBarsOperations_cancel(self):
        self.cancelRealTimeBars(3001)


    def marketDepthOperations_req(self):
        self.reqMktDepth(2001, ContractSamples.EurGbpFx(), 5, False, [])
        self.reqMktDepthExchanges()


    def marketDepthOperations_cancel(self):
        self.cancelMktDepth(2001, False)

    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True

    def stop(self):
        print("Executing cancels")
        #self.orderOperations_cancel()
        #self.accountOperations_cancel()
        #self.tickDataOperations_cancel()
        # self.marketDepthOperations_cancel()
        self.realTimeBarsOperations_cancel()
        #self.historicalDataOperations_cancel()
        #self.optionsOperations_cancel()
        #self.marketScanners_cancel()
        #self.fundamentalsOperations_cancel()
        #self.bulletinsOperations_cancel()
        #self.newsOperations_cancel()
        #self.pnlOperations_cancel()
        #self.histogramOperations_cancel()
        #self.continuousFuturesOperations_cancel()
        #self.tickByTickOperations_cancel()
        print("Executing cancels ... finished")

    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)

    # ! [error] self.reqId2nErr[reqId] += 1


    @iswrapper
    def winError(self, text: str, lastError: int):
        super().winError(text, lastError)




def main():
    SetupLogger()

    logging.debug("now is %s", datetime.datetime.now())
    logging.getLogger().setLevel(logging.ERROR)

    cmdLineParser = argparse.ArgumentParser("api tests")

    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                               dest="port", default=7496, help="The TCP port to use")
    cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
                               dest="global_cancel", default=False,
                               help="whether to trigger a globalCancel req")

    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)
    # print(args)


    # enable logging when member vars are assigned
    from ibapi import utils
    Order.__setattr__ = utils.setattr_log
    Contract.__setattr__ = utils.setattr_log
    DeltaNeutralContract.__setattr__ = utils.setattr_log
    TagValue.__setattr__ = utils.setattr_log
    TimeCondition.__setattr__ = utils.setattr_log
    ExecutionCondition.__setattr__ = utils.setattr_log
    MarginCondition.__setattr__ = utils.setattr_log
    PriceCondition.__setattr__ = utils.setattr_log
    PercentChangeCondition.__setattr__ = utils.setattr_log
    VolumeCondition.__setattr__ = utils.setattr_log



    # try:
    #     app = KRApp()
    #     if args.global_cancel:
    #         app.globalCancelOnly = True
    #     # ! [connect]
    #     app.connect("127.0.0.1", args.port, clientId=0)
    #     # ! [connect]
    #     print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
    #                                                   app.twsConnectionTime()))
    #
    #     app.marketDepthOperations_req()
    #     app.run()
    #     # app.realTimeBarsOperations_req()
    #
    # except:
    #     raise
    # finally:
    #     # app.dumpTestCoverageSituation()
    #     # app.dumpReqAnsErrSituation()
    #     ...


if __name__ == "__main__":
    main()