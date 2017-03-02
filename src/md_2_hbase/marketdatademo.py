#!/usr/bin/python#
# -*- coding:UTF-8 -*-

from ctp.ctp import *
import threading
import time
import sys
import os
import time

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

# Add path for local "gen-py/hbase" for the pre-generated module
gen_py_path = os.path.abspath('gen-py')
sys.path.append(gen_py_path)
from hbase import THBaseService
from hbase.ttypes import *



class TestMD(CThostFtdcMdSpi):
    m_nRequestID = 0
    m_szBrokerID = ""
    m_szUserID = ""
    m_szPassword = ""
    m_vecSubIntruments = []

    rowkeyindex = 1

    def __init__(self, front, szBrokerID, szUserID, szPassword,vecSubIntruments):
        CThostFtdcMdSpi.__init__(self)

        self.m_szBrokerID = szBrokerID
        self.m_szUserID = szUserID
        self.m_szPassword = szPassword
        self.m_vecSubIntruments = vecSubIntruments

        self.api = CThostFtdcMdApi_CreateFtdcMdApi("")
        self.api.RegisterSpi(self)
        self.api.RegisterFront(front)
        self.api.Init()

        return

    def join(self):
        self.api.Join()

    def OnFrontConnected(self):
        print("MD FrontConnected")
        field = CThostFtdcReqUserLoginField()
        field.BrokerID = self.m_szBrokerID
        field.UserID = self.m_szUserID
        field.Password = self.m_szPassword
        self.m_nRequestID += 1
        self.api.ReqUserLogin(field, self.m_nRequestID)
        return

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        print("MD OnRspUserLogin")
        print pRspInfo.ErrorMsg

        if (pRspInfo.ErrorID == 0):
            self.api.SubscribeMarketData(self.m_vecSubIntruments)
        return

    def OnRtnDepthMarketData(self, pDepthMarketData):
        #print pDepthMarketData.InstrumentID,pDepthMarketData.UpdateTime , pDepthMarketData.UpdateMillisec ,pDepthMarketData.LastPrice ,pDepthMarketData.Volume ,pDepthMarketData.AskPrice1 ,pDepthMarketData.AskVolume1 ,pDepthMarketData.BidPrice1 ,pDepthMarketData.BidVolume1

############################################################################################################################################################################
        try:
            ##
            ##  这里是把行情保存到hbase数据库中，
            ##  请修改下
            ###         host port
            ###         table 请在hbase shell中自行创建
            ##
            ##  补充说明：如果上生产系统，此处需要修改下，每次都创建socket close效率不高，把它移到外面去。（此处只是演示，so...）
            ##
            ##
            host = "127.0.0.1"      # todo 改成hbase的地址与端口
            port = 9090             # todo
            framed = False

            socket = TSocket.TSocket(host, port)
            if framed:
                transport = TTransport.TFramedTransport(socket)
            else:
                transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = THBaseService.Client(protocol)

            transport.open()

            table = "marketdata"     # todo

            rowkey = pDepthMarketData.InstrumentID + pDepthMarketData.TradingDay + pDepthMarketData.UpdateTime + '_' + str(self.rowkeyindex)
            self.rowkeyindex += 1
            put = TPut(row=rowkey, columnValues=
                                    [TColumnValue(family="data", qualifier="InstrumentID", value=pDepthMarketData.InstrumentID),
                                     TColumnValue(family="data", qualifier="TradingDay", value=pDepthMarketData.TradingDay),
                                     TColumnValue(family="data", qualifier="UpdateTime", value=pDepthMarketData.UpdateTime),
                                     TColumnValue(family="data", qualifier="UpdateMillisec", value=str(pDepthMarketData.UpdateMillisec)),
                                     TColumnValue(family="data", qualifier="LastPrice", value=str(pDepthMarketData.LastPrice)),
                                     TColumnValue(family="data", qualifier="Volume", value=str(pDepthMarketData.Volume)),
                                     TColumnValue(family="data", qualifier="AskPrice1", value=str(pDepthMarketData.AskPrice1)),
                                     TColumnValue(family="data", qualifier="AskVolume1", value=str(pDepthMarketData.AskVolume1)),
                                     TColumnValue(family="data", qualifier="BidPrice1", value=str(pDepthMarketData.BidPrice1)),
                                     TColumnValue(family="data", qualifier="BidPrice1", value=str(pDepthMarketData.BidVolume1)),
                                     ]
                       )
        
            client.put(table, put)

            transport.close()
        except Exception,e:
            print e

############################################################################################################################################################################

        return

    def __del__(self):
        self.api.RegisterSpi(None)
        self.api.Release()
        return

class TestTD(CThostFtdcTraderSpi):
    m_nRequestID = 0
    m_szBrokerID = ""
    m_szUserID = ""
    m_szPassword = ""
    m_vecIntruments = []

    def __init__(self, front, szBrokerID, szUserID, szPassword):
        CThostFtdcTraderSpi.__init__(self)

        self.m_szBrokerID = szBrokerID
        self.m_szUserID = szUserID
        self.m_szPassword = szPassword

        self.api = CThostFtdcTraderApi_CreateFtdcTraderApi("")
        self.api.RegisterSpi(self)
        self.api.SubscribePublicTopic(THOST_TERT_RESTART)
        self.api.SubscribePrivateTopic(THOST_TERT_RESTART)
        self.api.RegisterFront(front)
        self.api.Init()

        return

    def join(self):
        self.api.Join()

    def __del__(self):
        self.api.RegisterSpi(None)
        self.api.Release()
        return

    def TestInsertOrder(self):
        input = CThostFtdcInputOrderField( )
        input.BrokerID = self.m_strBrokerID
        input.InvestorID = self.m_strUserID
        input.InstrumentID = "rb1705"
        input.OrderRef = "2"
        input.OrderPriceType = THOST_FTDC_OPT_LimitPrice
        input.Direction = THOST_FTDC_D_Buy
        input.CombOffsetFlag = THOST_FTDC_OF_Open
        input.CombHedgeFlag = THOST_FTDC_HF_Speculation
        input.LimitPrice = 3100.00
        input.VolumeTotalOriginal = 1
        input.TimeCondition = THOST_FTDC_TC_GFD
        input.VolumeCondition = THOST_FTDC_VC_AV
        input.MinVolume = 1
        input.ContingentCondition = THOST_FTDC_CC_Immediately
        input.ForceCloseReason = THOST_FTDC_FCC_NotForceClose
        input.IsAutoSuspend = 1
        input.UserForceClose = 0

        self.m_nRequestID += 1
        return self.api.ReqOrderInsert(input, self.m_nRequestID)

    def OnFrontConnected(self):
        print("TD FrontConnected",self.m_szUserID)
        field = CThostFtdcReqUserLoginField()
        field.BrokerID = self.m_szBrokerID
        field.UserID = self.m_szUserID
        field.Password = self.m_szPassword
        self.m_nRequestID += 1
        self.api.ReqUserLogin(field, self.m_nRequestID)

        return

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspUserLogin")
        print pRspInfo.ErrorID,pRspInfo.ErrorMsg.decode("gb18030").encode("utf-8")
        print pRspUserLogin.MaxOrderRef

        item = CThostFtdcSettlementInfoConfirmField()
        item.BrokerID = self.m_szBrokerID
        item.InvestorID= self.m_szUserID
        self.m_nRequestID += 1
        self.api.ReqSettlementInfoConfirm(item, self.m_nRequestID)

        return


    def OnFrontDisconnected(self, nReason):
        print("TD OnFrontDisconnected")
        return

    def OnHeartBeatWarning(self, nTimeLapse):
        print("TD OnHeartBeatWarning")
        return 

    def OnRspAuthenticate(self, pRspAuthenticateField, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspAuthenticate")
        return 

    def OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspUserLogout")
        return 

    def OnRspUserPasswordUpdate(self, pUserPasswordUpdate, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspUserPasswordUpdate")
        return 

    def OnRspTradingAccountPasswordUpdate(self, pTradingAccountPasswordUpdate, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspTradingAccountPasswordUpdate")
        return

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspOrderInsert")
        return 

    def OnRspParkedOrderInsert(self, pParkedOrder, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspParkedOrderInsert")
        return

    def OnRspParkedOrderAction(self, pParkedOrderAction, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspParkedOrderAction")
        return

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspOrderAction")
        return

    def OnRspQueryMaxOrderVolume(self, pQueryMaxOrderVolume, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQueryMaxOrderVolume")
        return

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspSettlementInfoConfirm")

        req = CThostFtdcQryInstrumentField( )
        req.InstrumentID = ""
        self.m_nRequestID += 1
        print self.api.ReqQryInstrument( req, self.m_nRequestID )

        return

    def OnRspRemoveParkedOrder(self, pRemoveParkedOrder, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspRemoveParkedOrder")
        return

    def OnRspRemoveParkedOrderAction(self, pRemoveParkedOrderAction, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspRemoveParkedOrderAction")
        return

    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryOrder")
        return

    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTrade")
        return

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInvestorPosition")
        return

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTradingAccount")
        return

    def OnRspQryInvestor(self, pInvestor, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInvestor")
        return

    def OnRspQryTradingCode(self, pTradingCode, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTradingCode")
        return

    def OnRspQryInstrumentMarginRate(self, pInstrumentMarginRate, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInstrumentMarginRate")
        return

    def OnRspQryInstrumentCommissionRate(self, pInstrumentCommissionRate, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInstrumentCommissionRate")
        return

    def OnRspQryExchange(self, pExchange, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryExchange")
        return

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInstrument",pInstrument.InstrumentID )
        self.m_vecIntruments.append( pInstrument.InstrumentID )
        return

    def OnRspQryDepthMarketData(self, pDepthMarketData, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryDepthMarketData")
        return

    def OnRspQrySettlementInfo(self, pSettlementInfo, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQrySettlementInfo")
        return

    def OnRspQryTransferBank(self, pTransferBank, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTransferBank")
        return

    def OnRspQryInvestorPositionDetail(self, pInvestorPositionDetail, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInvestorPositionDetail")
        return

    def OnRspQryNotice(self, pNotice, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryNotice")
        return

    def OnRspQrySettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQrySettlementInfoConfirm")
        return

    def OnRspQryInvestorPositionCombineDetail(self, pInvestorPositionCombineDetail, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryInvestorPositionCombineDetail")
        return

    def OnRspQryCFMMCTradingAccountKey(self, pCFMMCTradingAccountKey, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryCFMMCTradingAccountKey")
        return

    def OnRspQryEWarrantOffset(self, pEWarrantOffset, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryEWarrantOffset")
        return

    def OnRspQryTransferSerial(self, pTransferSerial, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTransferSerial")
        return

    def OnRspQryAccountregister(self, pAccountregister, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryAccountregister")
        return

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspError")
        return

    def OnRtnOrder(self, pOrder):
        print("TD OnRtnOrder")
        return

    def OnRtnTrade(self, pTrade):
        print("TD OnRtnTrade")
        return

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        print("TD OnErrRtnOrderInsert")
        return

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        print("TD OnErrRtnOrderAction")
        return

    def OnRtnInstrumentStatus(self, pInstrumentStatus):
        print("TD OnRtnInstrumentStatus")
        return

    def OnRtnTradingNotice(self, pTradingNoticeInfo):
        print("TD OnRtnTradingNotice")
        return

    def OnRtnErrorConditionalOrder(self, pErrorConditionalOrder):
        print("TD OnRtnErrorConditionalOrder")
        return

    def OnRspQryContractBank(self, pContractBank, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryContractBank")
        return

    def OnRspQryParkedOrder(self, pParkedOrder, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryParkedOrder")
        return

    def OnRspQryParkedOrderAction(self, pParkedOrderAction, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryParkedOrderAction")
        return

    def OnRspQryTradingNotice(self, pTradingNotice, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryTradingNotice")
        return

    def OnRspQryBrokerTradingParams(self, pBrokerTradingParams, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryBrokerTradingParams")
        return

    def OnRspQryBrokerTradingAlgos(self, pBrokerTradingAlgos, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQryBrokerTradingAlgos")
        return

    def OnRtnFromBankToFutureByBank(self, pRspTransfer):
        print("TD OnRtnFromBankToFutureByBank")
        return

    def OnRtnFromFutureToBankByBank(self, pRspTransfer):
        print("TD OnRtnFromFutureToBankByBank")
        return

    def OnRtnRepealFromBankToFutureByBank(self, pRspRepeal):
        print("TD OnRtnRepealFromBankToFutureByBank")
        return

    def OnRtnRepealFromFutureToBankByBank(self, pRspRepeal):
        print("TD OnRtnRepealFromFutureToBankByBank")
        return

    def OnRtnFromBankToFutureByFuture(self, pRspTransfer):
        print("TD OnRtnFromBankToFutureByFuture")
        return

    def OnRtnFromFutureToBankByFuture(self, pRspTransfer):
        print("TD OnRtnFromFutureToBankByFuture")
        return

    def OnRtnRepealFromBankToFutureByFutureManual(self, pRspRepeal):
        print("TD OnRtnRepealFromBankToFutureByFutureManual")
        return

    def OnRtnRepealFromFutureToBankByFutureManual(self, pRspRepeal):
        print("TD OnRtnRepealFromFutureToBankByFutureManual")
        return

    def OnRtnQueryBankBalanceByFuture(self, pNotifyQueryAccount):
        print("TD OnRtnQueryBankBalanceByFuture")
        return

    def OnErrRtnBankToFutureByFuture(self, pReqTransfer, pRspInfo):
        print("TD OnErrRtnBankToFutureByFuture")
        return

    def OnErrRtnFutureToBankByFuture(self, pReqTransfer, pRspInfo):
        print("TD OnErrRtnFutureToBankByFuture")
        return

    def OnErrRtnRepealBankToFutureByFutureManual(self, pReqRepeal, pRspInfo):
        print("TD OnErrRtnRepealBankToFutureByFutureManual")
        return

    def OnErrRtnRepealFutureToBankByFutureManual(self, pReqRepeal, pRspInfo):
        print("TD OnErrRtnRepealFutureToBankByFutureManual")
        return

    def OnErrRtnQueryBankBalanceByFuture(self, pReqQueryAccount, pRspInfo):
        print("TD OnErrRtnQueryBankBalanceByFuture")
        return

    def OnRtnRepealFromBankToFutureByFuture(self, pRspRepeal):
        print("TD OnRtnRepealFromBankToFutureByFuture")
        return

    def OnRtnRepealFromFutureToBankByFuture(self, pRspRepeal):
        print("TD OnRtnRepealFromFutureToBankByFuture")
        return

    def OnRspFromBankToFutureByFuture(self, pReqTransfer, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspFromBankToFutureByFuture")
        return

    def OnRspFromFutureToBankByFuture(self, pReqTransfer, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspFromFutureToBankByFuture")
        return

    def OnRspQueryBankAccountMoneyByFuture(self, pReqQueryAccount, pRspInfo, nRequestID, bIsLast):
        print("TD OnRspQueryBankAccountMoneyByFuture")
        return

    def OnRtnOpenAccountByBank(self, pOpenAccount):
        print("TD OnRtnOpenAccountByBank")
        return

    def OnRtnCancelAccountByBank(self, pCancelAccount):
        print("TD OnRtnCancelAccountByBank")
        return

    def OnRtnChangeAccountByBank(self, pChangeAccount):
        print("TD OnRtnChangeAccountByBank")
        return

##########################################################################################################################

userid = raw_input("UserID: ")
passwd = raw_input("Password: ")

tdSpi = TestTD("tcp://180.168.146.187:10000", "9999", userid, passwd)
time.sleep( 5 )
print tdSpi.m_vecIntruments

mdSpi = TestMD("tcp://180.168.146.187:10010", "9999", userid, passwd, tdSpi.m_vecIntruments)

tdSpi.join()




