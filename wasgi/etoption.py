

from   scipy.stats import norm
import finpie.price_data
import finpie
import yahoo_fin.options as ops
import datetime
import numpy as np
import mysql.connector
import pandas.util.testing as tm
import pandas as pd
from   yahoo_fin import stock_info as si
import robin_stocks.robinhood as rh
import scipy
import scipy.optimize
from   scipy.stats import norm
import gzip
import json
import glob
import os
import requests
import time

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import importlib
import stockquotes
import yfinance as yf
import mibian
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline

class etoption:
    db=None
    dbcursor=None


    spxlist=['A', 'AAL', 'AAP', 'AAPL', 'ABBV', 'ABC', 'ABMD', 'ABT', 'ACN', 'ADBE', 'ADI', 'ADM', 'ADP', 'ADSK', 'AEE', 'AEP', 'AES', 'AFL', 'AIG', 'AIZ', 
        'AJG', 'AKAM', 'ALB', 'ALGN', 'ALK', 'ALL', 'ALLE', 'ALXN', 'AMAT', 'AMCR', 'AMD', 'AME', 'AMGN', 'AMP', 'AMT', 'AMZN', 'ANET', 'ANSS', 'ANTM', 'AON', 
        'AOS', 'APA', 'APD', 'APH', 'APTV', 'ARE', 'ATO', 'ATVI', 'AVB', 'AVGO', 'AVY', 'AWK', 'AXP', 'AZO', 'BA', 'BAC', 'BAX', 'BBY', 'BDX', 'BEN', 
        'BF.B', 'BIIB', 'BIO', 'BK', 'BKNG', 'BKR', 'BLK', 'BLL', 'BMY', 'BR', 'BRK.B', 'BSX', 'BWA', 'BXP', 'C', 'CAG', 'CAH', 'CARR', 'CAT', 'CB', 
        'CBOE', 'CBRE', 'CCI', 'CCL', 'CDNS', 'CDW', 'CE', 'CERN', 'CF', 'CFG', 'CHD', 'CHRW', 'CHTR', 'CI', 'CINF', 'CL', 'CLX', 'CMA', 'CMCSA', 'CME', 
        'CMG', 'CMI', 'CMS', 'CNC', 'CNP', 'COF', 'COG', 'COO', 'COP', 'COST', 'CPB', 'CPRT', 'CRL', 'CRM', 'CSCO', 'CSX', 'CTAS', 'CTLT', 'CTSH', 'CTVA', 
        'CTXS', 'CVS', 'CVX', 'CZR', 'D', 'DAL', 'DD', 'DE', 'DFS', 'DG', 'DGX', 'DHI', 'DHR', 'DIS', 'DISCA', 'DISCK', 'DISH', 'DLR', 'DLTR', 'DOV', 
        'DOW', 'DPZ', 'DRE', 'DRI', 'DTE', 'DUK', 'DVA', 'DVN', 'DXC', 'DXCM', 'EA', 'EBAY', 'ECL', 'ED', 'EFX', 'EIX', 'EL', 'EMN', 'EMR', 'ENPH', 
        'EOG', 'EQIX', 'EQR', 'ES', 'ESS', 'ETN', 'ETR', 'ETSY', 'EVRG', 'EW', 'EXC', 'EXPD', 'EXPE', 'EXR', 'F', 'FANG', 'FAST', 'FB', 'FBHS', 'FCX', 
        'FDX', 'FE', 'FFIV', 'FIS', 'FISV', 'FITB', 'FLT', 'FMC', 'FOX', 'FOXA', 'FRC', 'FRT', 'FTNT', 'FTV', 'GD', 'GE', 'GILD', 'GIS', 'GL', 'GLW', 
        'GM', 'GNRC', 'GOOG', 'GOOGL', 'GPC', 'GPN', 'GPS', 'GRMN', 'GS', 'GWW', 'HAL', 'HAS', 'HBAN', 'HBI', 'HCA', 'HD', 'HES', 'HIG', 'HII', 'HLT', 
        'HOLX', 'HON', 'HPE', 'HPQ', 'HRL', 'HSIC', 'HST', 'HSY', 'HUM', 'HWM', 'IBM', 'ICE', 'IDXX', 'IEX', 'IFF', 'ILMN', 'INCY', 'INFO', 'INTC', 'INTU', 
        'IP', 'IPG', 'IPGP', 'IQV', 'IR', 'IRM', 'ISRG', 'IT', 'ITW', 'IVZ', 'J', 'JBHT', 'JCI', 'JKHY', 'JNJ', 'JNPR', 'JPM', 'K', 'KEY', 'KEYS', 
        'KHC', 'KIM', 'KLAC', 'KMB', 'KMI', 'KMX', 'KO', 'KR', 'KSU', 'L', 'LB', 'LDOS', 'LEG', 'LEN', 'LH', 'LHX', 'LIN', 'LKQ', 'LLY', 'LMT', 
        'LNC', 'LNT', 'LOW', 'LRCX', 'LUMN', 'LUV', 'LVS', 'LW', 'LYB', 'LYV', 'MA', 'MAA', 'MAR', 'MAS', 'MCD', 'MCHP', 'MCK', 'MCO', 'MDLZ', 'MDT', 
        'MET', 'MGM', 'MHK', 'MKC', 'MKTX', 'MLM', 'MMC', 'MMM', 'MNST', 'MO', 'MOS', 'MPC', 'MPWR', 'MRK', 'MRO', 'MS', 'MSCI', 'MSFT', 'MSI', 'MTB', 
        'MTD', 'MU', 'MXIM', 'NCLH', 'NDAQ', 'NEE', 'NEM', 'NFLX', 'NI', 'NKE', 'NLOK', 'NLSN', 'NOC', 'NOV', 'NOW', 'NRG', 'NSC', 'NTAP', 'NTRS', 'NUE', 
        'NVDA', 'NVR', 'NWL', 'NWS', 'NWSA', 'NXPI', 'O', 'ODFL', 'OGN', 'OKE', 'OMC', 'ORCL', 'ORLY', 'OTIS', 'OXY', 'PAYC', 'PAYX', 'PBCT', 'PCAR', 'PEAK', 
        'PEG', 'PENN', 'PEP', 'PFE', 'PFG', 'PG', 'PGR', 'PH', 'PHM', 'PKG', 'PKI', 'PLD', 'PM', 'PNC', 'PNR', 'PNW', 'POOL', 'PPG', 'PPL', 'PRGO', 
        'PRU', 'PSA', 'PSX', 'PTC', 'PVH', 'PWR', 'PXD', 'PYPL', 'QCOM', 'QRVO', 'RCL', 'RE', 'REG', 'REGN', 'RF', 'RHI', 'RJF', 'RL', 'RMD', 'ROK', 
        'ROL', 'ROP', 'ROST', 'RSG', 'RTX', 'SBAC', 'SBUX', 'SCHW', 'SEE', 'SHW', 'SIVB', 'SJM', 'SLB', 'SNA', 'SNPS', 'SO', 'SPG', 'SPGI', 'SRE', 'STE', 
        'STT', 'STX', 'STZ', 'SWK', 'SWKS', 'SYF', 'SYK', 'SYY', 'T', 'TAP', 'TDG', 'TDY', 'TEL', 'TER', 'TFC', 'TFX', 'TGT', 'TJX', 'TMO', 'TMUS', 
        'TPR', 'TRMB', 'TROW', 'TRV', 'TSCO', 'TSLA', 'TSN', 'TT', 'TTWO', 'TWTR', 'TXN', 'TXT', 'TYL', 'UA', 'UAA', 'UAL', 'UDR', 'UHS', 'ULTA', 'UNH', 
        'UNM', 'UNP', 'UPS', 'URI', 'USB', 'V', 'VFC', 'VIAC', 'VLO', 'VMC', 'VNO', 'VRSK', 'VRSN', 'VRTX', 'VTR', 'VTRS', 'VZ', 'WAB', 'WAT', 'WBA', 
        'WDC', 'WEC', 'WELL', 'WFC', 'WHR', 'WLTW', 'WM', 'WMB', 'WMT', 'WRB', 'WRK', 'WST', 'WU', 'WY', 'WYNN', 'XEL', 'XLNX', 'XOM', 'XRAY', 'XYL', 
        'YUM', 'ZBH', 'ZBRA', 'ZION']
    qqqlist= ['AAPL','MSFT','AMZN','GOOG','FB','TSLA','NVDA','GOOGL','PYPL','ADBE','CMCSA','NFLX','CSCO','INTC','PEP','AVGO','TMUS','COST','TXN','QCOM',
        'AMGN','CHTR','INTU','SBUX','AMAT','ISRG','AMD','MRNA','ZM','BKNG','MDLZ','MU','LRCX','ADP','GILD','MELI','FISV','ATVI','CSX','ILMN',
        'ADSK','ADI','REGN','JD','IDXX','DOCU','BIIB','NXPI','ASML','VRTX','KDP','ALGN','KHC','MNST','EBAY','LULU','KLAC','MRVL','BIDU','MAR',
        'WDAY','EXC','ROST','DXCM','MTCH','AEP','SNPS','PDD','CTAS','ORLY','ALXN','WBA','EA','PAYX','MCHP','CDNS','XEL','TEAM','CTSH','PTON',
        'XLNX','CPRT','NTES','OKTA','SWKS','ANSS','FAST','PCAR','VRSK','SGEN','SIRI','MXIM','VRSN','CDW','CERN','DLTR','SPLK','INCY','TCOM','CHKP',
        'FOXA','FOX']

    def __init__(self,database='finance',user='etam89',passwd='password'):
        display(HTML("<style>.container { width:100% !important; }</style>"))
        plt.rcParams['figure.figsize'] = [20, 8]
        plt.rcParams['figure.dpi'] = 100

        self.db_init(database=database,user=user,passwd=passwd)
        
    def getdbcursor(self):
        return self.dbcursor

    def ExpectedVal(self,x):
        v=np.exp(-x*x/2)/np.sqrt(2*np.pi)
        if x>0:
            return v
        else:
            return -v
    # def sell_put_loss(x):
    #     return ExpectedVal(x)-x*norm.cdf(x)
    # def sell_call_loss(x):
    #     return -ExpectedVal(x)+x*(1-norm.cdf(x))
    def sell_put_loss(self,x,mu,sigma):
        x=(x-mu)/mu/sigma
        return (self.ExpectedVal(x)-x*norm.cdf(x))*mu*sigma
    def sell_call_loss(self,x,mu,sigma):
        x=(x-mu)/mu/sigma
        return (-self.ExpectedVal(x)+x*(1-norm.cdf(x)))*mu*sigma

    def diffdays(self,d1,d2):
        return (datetime.datetime.strptime(d1,"%Y-%m-%d")-datetime.datetime.strptime(d2,"%Y-%m-%d")).days
    
    def db_init(self,database='',usedbpool=False,host='localhost',user='etam89',passwd='password'):
        if 'FINANCE_DB_USER' in os.environ.keys():
            user=os.environ['FINANCE_DB_USER']
        if 'FINANCE_DB_PASSWD' in os.environ.keys():
            passwd=os.environ['FINANCE_DB_PASSWD']
        self.db=mysql.connector.connect(host=host,user=user,passwd=passwd,database=database)
        self.dbcursor=self.db.cursor()
        print(self.db)
    
    def insert2Field(self,tbl,fld,vtype,val):
        sql="insert ignore into "+tbl+"("+fld+") values('"
        if len(val)>0:
            for v in val:
                s=sql+"','".join(str(i) for i in v)+"')"
                self.dbcursor.execute(s)
            self.db.commit()

    def insertDictArray(self,tbl,arr):
        keys=arr[0].keys()
        fld='`'+'`,`'.join(keys)+'`'
        vtype=','.join(['%s']*len(keys))
        val= [[a[k] for k in keys] for a in arr]
        self.insert2Field(tbl,fld,vtype,val)

    def db_fetch(self,sql):
        self.dbcursor.execute(sql)
        return self.dbcursor.fetchall()

    def db_fetch_dict(self,sql):
        self.dbcursor.execute(sql)
        col=[col[0] for col in self.dbcursor.description]
        return [dict(zip(col,row)) for row in self.dbcursor.fetchall()]
    
    
    def get_tradedate(self):
        d=datetime.datetime.today()
        if datetime.datetime.today().time().hour <6:
            d=d- datetime.timedelta(days=1)
        return d.date().strftime("%Y-%m-%d")

    def db_get_symbols(self,db="optionyh"):
        self.dbcursor.execute("select distinct symbol from "+db+" order by symbol desc")
        return [x[0] for x in self.dbcursor.fetchall()]
    
    def dl2db_finpie(self,symlist,tradedate,db='option_yhyh'):
        bad=[]
        for sym in symlist:
            print(sym+"    ",end="")
            try:
               opt=finpie.yahoo_option_chain(sym)
            except Exception as ex:
                opt=None
                print(str(ex))
            cnt=0
            if not opt is None:
                for i in range(0,2):
                    if i==0:
                        typ='call'
                    else:
                        typ='put'
     
    #                    result=[tuple([sym,tradedate,typ])+tuple(r[0:2])+tuple(r[3:10])+tuple([r[11].strftime('%Y-%m-%d'),r[12].strftime('%Y-%m-%d'),r[13]]) for r in opt[i].to_numpy() if len(r)>=15 and r[14] !='nan' and r[10]=='REGULAR']
    #                    result1=[tuple([sym,tradedate,typ])+tuple(r[0:2])+tuple(r[3:8])+tuple([r[13],r[12]])+tuple([r[9].strftime('%Y-%m-%d'),r[10].strftime('%Y-%m-%d'),r[11]]) for r in opt[i].to_numpy() if len(r)>=15 and r[14] !='nan' and r[8]=='REGULAR']
    #                    result=result+result1
                    result=[]
                    opt[i].fillna(0,inplace=True)
                    for ind in range(opt[i].shape[0]):
                        try:
                            result.append(tuple([sym,tradedate,typ,opt[i].iloc[ind]['contract_symbol']
                                ,opt[i].iloc[ind]['strike']
                                ,opt[i].iloc[ind]['last_price']
                                ,opt[i].iloc[ind]['change']
                                ,opt[i].iloc[ind]['percent_change']
                                ,opt[i].iloc[ind]['volume']
                                ,opt[i].iloc[ind]['open_interest']
                                ,opt[i].iloc[ind]['bid']
                                ,opt[i].iloc[ind]['ask']
                                ,opt[i].iloc[ind]['expiration'].strftime('%Y-%m-%d')
                                ,opt[i].iloc[ind]['last_trade_date'].strftime('%Y-%m-%d')
                                ,opt[i].iloc[ind]['implied_volatility']]))
                        except:
                            pass
    #                    print([tuple([sym,tradedate,typ])+tuple(r[0:2])+tuple(r[3:10])+tuple([r[11].strftime('%Y-%m-%d'),r[12].strftime('%Y-%m-%d'),r[13]]) for r in opt[i].to_numpy() if len(r)<15 or r[14] =='nan'])
                    try:
                        self.insert2Field(db,'symbol,tradedate,type,contract_symbol ,strike ,last_price ,chg ,percent_change ,volume ,open_interest ,bid ,ask ,expiration ,last_trade_date ,implied_volatility',
                             '%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s', result)
                        cnt=cnt+len(result)
                    except:
                        bad.append(sym)
                        pass
#                   print(len(result))
            print(cnt)
        return(bad)

    def dl2db_finpie_rt(self,symlist,tradetime,db='option_yh_time'):
        bad=[]
        tradedate=tradetime.strftime('%Y-%m-%d')
        tradetime=tradetime.strftime('%Y-%m-%d %H:%M:%S')
        for sym in symlist:
            print(sym+"    ",end="")
            try:
                stock=stockquotes.Stock(sym)
                opt=finpie.yahoo_option_chain(sym)
            except Exception as ex:
                opt=None
            cnt=0
            if not opt is None:
                for i in range(0,2):
                    if i==0:
                        typ='call'
                    else:
                        typ='put'

                    result=[]
                    opt[i]['stockprice']=[stock.current_price]*opt[i].shape[0]
                    opt[i].fillna(0,inplace=True)
                    for ind in range(opt[i].shape[0]):
                        try:
                            result.append(tuple([sym,stock.current_price,tradedate,tradetime,typ,opt[i].iloc[ind]['contract_symbol']
                                ,opt[i].iloc[ind]['strike']
                                ,opt[i].iloc[ind]['last_price']
                                ,opt[i].iloc[ind]['change']
                                ,opt[i].iloc[ind]['percent_change']
                                ,opt[i].iloc[ind]['volume']
                                ,opt[i].iloc[ind]['open_interest']
                                ,opt[i].iloc[ind]['bid']
                                ,opt[i].iloc[ind]['ask']
                                ,opt[i].iloc[ind]['expiration'].strftime('%Y-%m-%d')
                                ,opt[i].iloc[ind]['last_trade_date'].strftime('%Y-%m-%d')
                                ,opt[i].iloc[ind]['implied_volatility']]))
                        except Exception as ex:
                            pass
                    try:
                        self.insert2Field(db,'symbol,stockprice,tradedate,tradetime,type,contract_symbol ,strike ,last_price ,chg ,percent_change ,volume ,open_interest ,bid ,ask ,expiration ,last_trade_date ,implied_volatility',
                             '%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s', result)
                        cnt=cnt+len(result)
                    except Exception as ex:
                        bad.append(sym)
                        pass
            print(cnt)
        return(bad)

    def login_rh(self):
        password=input("password:")
        rh.authentication.login("abb04@yahoo.com",password)

    def dl2db_rh(self,symlist,tradedate,expiration):
        for sym in symlist:
            print(sym)
            data=[None,None]
            try:
                calls=[{**rh.get_option_market_data_by_id(x['id'])[0],**rh.get_option_instrument_data_by_id(x['id'])} for x in rh.find_tradable_options(sym,expirationDate=expiration,optionType='call')]
                puts =[{**rh.get_option_market_data_by_id(x['id'])[0],**rh.get_option_instrument_data_by_id(x['id'])} for x in rh.find_tradable_options(sym,expirationDate=expiration,optionType='put')]
                data[0]=[{'contract_symbol':x['id'],'strike':float(x['strike_price']),'last_price':(0 if x['last_trade_price'] is None else float(x['last_trade_price'])),'volume':x['volume'],'open_interest':x['open_interest'],'bid':float(x['bid_price']),'ask':float(x['ask_price']),'expiration':x['expiration_date'],'last_trade_date':tradedate,'implied_volatility':(0.0 if x['implied_volatility'] is None else float(x['implied_volatility'])),'symbol':x['symbol'],'trade_date':tradedate,'type':x['type']} for x in calls]
                data[1]=[{'contract_symbol':x['id'],'strike':float(x['strike_price']),'last_price':(0 if x['last_trade_price'] is None else float(x['last_trade_price'])),'volume':x['volume'],'open_interest':x['open_interest'],'bid':float(x['bid_price']),'ask':float(x['ask_price']),'expiration':x['expiration_date'],'last_trade_date':tradedate,'implied_volatility':(0.0 if x['implied_volatility'] is None else float(x['implied_volatility'])),'symbol':x['symbol'],'trade_date':tradedate,'type':x['type']} for x in puts]
            except Exception as ex:
                data[0]=None
                data[1]=None
                print(str(ex))
            typ=['call','put']
            if (not data[0] is None) and (not data[1] is None):
                for i in range(2):
                    result=[]
                    for r in data[i]:
                        try:
                            result.append(tuple([sym,tradedate,typ[i]
                                ,r['contract_symbol']
                                ,r['strike']
                                ,r['last_price']
                                ,r['volume']
                                ,r['open_interest']
                                ,r['bid']
                                ,r['ask']
                                ,r['expiration']
                                ,r['trade_date']
                                ,r['implied_volatility']]))
                        except:
                            pass
                    try:
                        self.insert2Field('option_yh','symbol,tradedate,type,contract_symbol ,strike ,last_price ,volume ,open_interest ,bid ,ask ,expiration ,last_trade_date ,implied_volatility',
                             '%s %s %s %s %s %s %s %s %s %s %s %s %s', result)
                    except:
                        pass

    def fetch_optionyh(self,sym,typ=None,strike=None,tradedate=None,expiration=None):
        if typ is not None:
            typ=" type = '"+typ+"' "
        else: 
            typ="1"
        if strike is not None:
            strike=" strike = '"+str(strike)+"' "  
        else:
            strike="1"
        if tradedate == 'recent':
            self.dbcursor.execute("select max(tradedate) from option_yh where "+ " and ".join({'1',typ,strike}))
            tradedate=self.dbcursor.fetchall()[0][0].strftime("%Y-%m-%d")
        elif tradedate is not None:
            tradedate=" tradedate = '"+tradedate+"' "  
        else:
            tradedate="1"
        if expiration is not None:
            expiration=" expiration = '"+expiration+"' "  
        else:
            expiration="1"

#       self.dbcursor.execute("select *  from option_yh where "+ " and ".join({'1',typ,strike,tradedate,expiration," symbol = '"+sym+"' "}))
        self.dbcursor.execute("select *  from option_yh where "+ " and ".join(["symbol='"+sym+"'",typ,strike,tradedate,expiration])+"order by symbol,type,expiration,tradedate,strike")
        columns=self.dbcursor.description
        result= [{columns[index][0]:column for index, column in enumerate(value)} for value in self.dbcursor.fetchall()]
        for v in result:
            v['last_trade_date']=v['last_trade_date'].strftime('%Y-%m-%d')
            v['tradedate']=v['tradedate'].strftime('%Y-%m-%d')
            v['expiration']=v['expiration'].strftime('%Y-%m-%d')
        return result

    def fetch_optioncboe(self,sym,typ=None,strike=None,tradedate=None,expiration=None):
        if typ is not None:
            typ=" type = '"+typ+"' "
        else: 
            typ="1"
        if strike is not None:
            strike=" strike = '{}'".format(strike)
        else:
            strike="1"
        if tradedate == 'recent':
            self.dbcursor.execute("select max(tradedate) from cboe_option where "+ " and ".join({'1',typ,strike}))
            tradedate=self.dbcursor.fetchall()[0][0].strftime("%Y-%m-%d")
        elif tradedate is not None:
            tradedate=" tradedate = '"+tradedate+"' "  
        else:
            tradedate="1"
        if expiration is not None:
            expiration=" expiration = '"+expiration+"' "  
        else:
            expiration="1"

#       print("select *  from cboe_option where "+ " and ".join(["symbol='"+sym+"'",typ,strike,tradedate,expiration])+"order by symbol,type,expiration,tradedate,strike")
        self.dbcursor.execute("select *  from cboe_option where "+ " and ".join(["symbol='"+sym+"'",typ,strike,tradedate,expiration])+"order by symbol,type,expiration,tradedate,strike")
        columns=self.dbcursor.description
        result= [{columns[index][0]:column for index, column in enumerate(value)} for value in self.dbcursor.fetchall()]
        for v in result:
            v['last_trade_date']=v['tradedate'].strftime('%Y-%m-%d')
            v['tradedate']=v['tradedate'].strftime('%Y-%m-%d')
            v['expiration']=v['expiration'].strftime('%Y-%m-%d')
            v['implied_volatility']=v['iv']
            v['last_price']=(v['bid']+v['ask'])/2
#       print(result)
        return result

    def fetch_atm(self,sym,price,tradedate,expiration,cnt=1):
        if price is None:
            price=self.db_fetch("select close form cboe_stock where symbol='"+sym+"' and tradedate='"+tradedate+"'")
            if len(price)==0:
                return [None,None]
            price=price[0]
        price=str(price)
        lcnt=str(cnt)
        result=self.db_fetch_dict("select *  from cboe_option where symbol='"+ sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+ "' and type='call' order by abs(strike - "+price+") asc limit "+lcnt)
        result=result+self.db_fetch_dict("select *  from cboe_option where symbol='"+ sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+ "' and type='put'  order by abs(strike - "+price+") asc limit "+lcnt)
        for v in range(len(result)):
            result[v]['last_trade_date']=result[v]['tradedate'].strftime('%Y-%m-%d')
            result[v]['tradedate']=result[v]['tradedate'].strftime('%Y-%m-%d')
            result[v]['expiration']=result[v]['expiration'].strftime('%Y-%m-%d')
            result[v]['implied_volatility']=result[v]['iv']
            result[v]['last_price']=(result[v]['bid']+result[v]['ask'])/2

        return result

#    def fetch_atm_volatility(self,sym,price,tradedate,expiration):
#        iv=self.fetch_atm(sym,price,tradedate,expiration,cnt=3)
#        avg_iv=[]
#        c_iv=np.array([x['implied_volatility'] for x in iv[:3] if x['implied_volatility'] !=0])
#        if len(c_iv)>0:
#            avg_iv.append(np.min(c_iv))
#        p_iv=np.array([x['implied_volatility'] for x in iv[3:] if x['implied_volatility'] !=0])
#        if len(p_iv)>0:
#            avg_iv.append(np.min(p_iv))
#        return np.average(np.array(avg_iv))

    def fetch_atm_iv(self,sym,tradedate,expiration):
        rec=self.db_fetch("select iv from cboe_option where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and iv <>0 order by abs(strike - close) asc limit 2")
        if len(rec)==0:
            return None
        elif len(rec)==1:
            return rec[0][0]
        else:
            return (rec[0][0]+rec[1][0])/2

    def fetch_recent_price(self,sym,db='stock'):
        if db=='cboe_stock':
            datestr='tradedate'
        else:
            datestr='date'
        self.dbcursor.execute("select * from "+db+" where symbol='"+sym+"'  order by "+datestr+" limit 1")
        columns=self.dbcursor.description
        result= [{columns[index][0]:column for index, column in enumerate(value)} for value in self.dbcursor.fetchall()]
        if len(result)==0:
            return None
        result=result[0]
        result[datestr]=result[datestr].strftime('%Y-%m-%d')
        return result

    def fetch_near_expiration(self,sym, db='cboe_option'):
        self.dbcursor.execute("select min(expiration) from "+db+" where symbol='"+sym+"'  and expiration>='"+datetime.date.today().strftime('%Y-%m-%d')+"'")
        return self.dbcursor.fetchall()[0][0].strftime("%Y-%m-%d")

    def fetch_near_strike(self,sym):
        self.dbcursor.execute("select strike  from cboe_option where symbol='"+sym+"'  and expiration>='"+datetime.date.today().strftime('%Y-%m-%d')+"' order by  tradedate desc, expiration asc , abs(close-strike) asc limit 1")
        return self.dbcursor.fetchall()[0][0]

    def fetch_near_tradedate(self,sym):
        print(sym)
        self.dbcursor.execute("select tradedate  from cboe_option where symbol='"+sym+"'   order by  tradedate desc limit 1")
        return self.dbcursor.fetchall()[0][0]

    def fetch_recent_optiondate(self,sym):
        self.dbcursor.execute("select max(tradedate) from option_yh where symbol='"+sym+"'")
        return self.dbcursor.fetchall()[0][0].strftime("%Y-%m-%d")

#   history: 'auto', True,False
    def dlstock2db_yf(self,symlist,db='stock'):
        for sym in symlist:
            print(sym+' ',end='')
            try:
                df=yf.download(sym,progress=False)
                df['Date']=df.index
                df['Symbol']=sym
                s=df.to_dict(orient='records')
                result=[(v['Symbol'],v['Date'],v['Open'],v['Low'],v['High'],v['Close'],v['Adj Close'],v['Volume']) for v in s]
                print(len(result))
                if len(result)>0:
                    self.insert2Field(db,'symbol,date,open,low,high,close,adj_close,volume',
                             '%s %s %s %s %s %s %s %s', result)
            except Exception as ex:
                print(str(ex))
                pass

#   history: 'auto', True,False
    def dlstock2db(self,symlist,history='auto'):
        for sym in symlist:
            print(sym+' ',end='')
            try:
                S=si.get_quote_table(sym)
                if S['Quote Price'] !=S['Quote Price']:
                    S=None
                    history=True
            except Exception as ex:
                S=None
                history=True
    
            if history=='auto':
                self.dbcursor.execute("select 1 from stock where symbol='"+sym+"' limit 1")
                if len(self.dbcursor.fetchall())>0:
                    history=False
                else:
                    history=True
            try:
                if history:
                    s=finpie.historical_prices(sym)
                    s.reset_index(inplace=True)
                    s['symbol']=sym
                    s['date']=s['date'].map(lambda x: x.strftime("%Y-%m-%d"))
                    s=s.to_dict('records')
                else:
                    s=[]
            except Exception as ex:
                print(str(ex))
                s=None
            if not s is None:
                if S is not None:
                    S=dict(zip(['symbol','date','open','low','high','close','adj_close','volume'],[sym,self.get_tradedate(),S['Open'],*np.array(S["Day's Range"].split())[[0,2]],S['Quote Price'],S['Quote Price'],int(S['Volume'])]))
                    s.append(S)
                result=[(v['symbol'],v['date'],v['open'],v['low'],v['high'],v['close'],v['adj_close'],v['volume']) for v in s]
                print(len(result))
                try:
                    if len(result)>0:
                        self.insert2Field('stock','symbol,date,open,low,high,close,adj_close,volume',
                             '%s %s %s %s %s %s %s %s', result)
                except Exception as ex:
                    print(str(ex))
                    pass
    def option_find_strangle(self,sym,tradedate,expiration,call_sig=2,put_sig=2,usedb='cboe_option'):
        try:
            c=self.db_fetch('select iv,close from cboe_option where symbol=\''+sym+'\' and type=\'call\' and expiration=\''+expiration+'\' and tradedate=\''+tradedate+'\' and iv<>0 order by abs(close-strike) limit 1')[0]
            p=self.db_fetch('select iv,close from cboe_option where symbol=\''+sym+'\' and type=\'put\' and expiration=\''+expiration+'\' and tradedate=\''+tradedate+'\' and iv<>0 order by abs(close-strike) limit 1')[0]
            csig_anual=c[0];
            psig_anual=p[0];
            iv=(c[0]+p[0])/2
            daydiff=(datetime.datetime.strptime(expiration,'%Y-%m-%d')-datetime.datetime.strptime(tradedate,'%Y-%m-%d')).days
            iv=iv*np.sqrt(daydiff/365)
            close=c[1]
            tgt=[close-iv*put_sig*close,close+iv*call_sig*close]
            c=self.db_fetch('select bid, strike,close from cboe_option where symbol=\''+sym+'\' and type=\'call\' and expiration=\''+expiration+'\' and tradedate=\''+tradedate+'\' order by abs(strike-'+str(tgt[1])+') limit 1')[0]
            p=self.db_fetch('select bid, strike,close from cboe_option where symbol=\''+sym+'\' and type=\'put\'  and expiration=\''+expiration+'\' and tradedate=\''+tradedate+'\' order by abs(strike-'+str(tgt[0])+') limit 1')[0]
            ret=(c[0]+p[0])/(close/5)
            return {'symbol':sym,'tradedate':tradedate,'expiration':expiration,'close':close,'anual_sigma_call':csig_anual,'anual_sigma_put':psig_anual,'days':daydiff,'period_iv': iv,'call_strike':tgt[1],'put_strike':tgt[0],'call_bid':c[0],'put_bid':p[0],'return':ret}
        except:
            return None

    def option_strangle(self,sym,tradedate=None,expiration=None,usedb='option_yh'):
        if usedb=='cboe_option':
            fetchfunc=self.fetch_optioncboe
        else:
            fetchfunc=self.fetch_optionyh
        stock=self.fetch_db_stock(sym,db='cboe_stock',startdate=tradedate,enddate=tradedate)
        S=stock[0]['close']
#       S=self.fetch_recent_price(sym)['close']
        if tradedate is None:
            tradedate=self.fetch_recent_optiondate(sym)
        if expiration is None:
            expiration=self.fetch_near_expiration(sym)
        calls=[x for x in fetchfunc(sym,expiration=expiration,typ='call',tradedate=tradedate) if not x['implied_volatility']==0 and x['strike'] >=S]
        puts= [x for x in fetchfunc(sym,expiration=expiration,typ='put',tradedate=tradedate) if not x['implied_volatility']==0 and x['strike']<=S ]
        print(len(calls))
        print(len(puts))
#        IV=np.max([calls[np.argmin(np.array([np.abs(x['strike']-S) for x in calls]))]['implied_volatility'],puts[np.argmin(np.array([np.abs(x['strike']-S) for x in puts]))]['implied_volatility']])
        IV=np.max([x['implied_volatility'] for x in calls[:2]] + [x['implied_volatility'] for x in puts[-2:]])
        t=(datetime.datetime.strptime(expiration,"%Y-%m-%d")-datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)).days
        if t==0:
            t=1
        IV=IV/np.sqrt(365/t)
#       sellcall=[[r['strike'],r['last_price'],self.sell_call_loss(r['strike'],S,IV)] for r in calls ]
        sellcall=[[r['strike'],r['bid'],self.sell_call_loss(r['strike'],S,IV)] for r in calls ]
#       sellput=[[r['strike'],r['last_price'],self.sell_put_loss(r['strike'],S,IV)] for r in puts]
        sellput=[[r['strike'],r['bid'],self.sell_put_loss(r['strike'],S,IV)] for r in puts]
        lst=[(['-']*3)+['Strike']+[x[0] for x in sellcall]]
        lst=lst+[(['-']*3)+['Premium']+[x[1] for x in sellcall]]
        lst=lst+[(['-']*3)+['Risk']+[x[2] for x in sellcall]]
        lst=lst+[['Strike','Premium','Risk','Sigma']+[np.abs(x[0]-S)/S/IV for x in sellcall]]
        lst=lst+[p+[np.abs((p[0]-S)/S/IV)]+[x[1]+x[2]+p[1]+p[2] for x in sellcall] for p in sellput][::-1]
        df=pd.DataFrame(lst,columns=['Strike','Premium','Risk','Sigma']+[x[0] for x in sellcall])
        return expiration,tradedate,S,IV,df

    def option_strangle_online(self,sym):
        S=si.get_live_price(sym)
        optiondate=d=datetime.date.today().strftime("%Y-%m-%d")
        expiration=datetime.datetime.strftime(datetime.datetime.strptime(ops.get_expiration_dates(sym)[0],"%B %d, %Y"),"%Y-%m-%d")
        calls=[x for _,x in ops.get_calls(sym,date=expiration).iterrows() if not x['Implied Volatility']==0 and x['Strike'] >=S]
        puts =[x for _,x in ops.get_puts(sym,date=expiration).iterrows() if not x['Implied Volatility']==0 and x['Strike'] <=S]
        IV=np.average([float(calls[np.argmin(np.array([np.abs(x['Strike']-S) for x in calls]))]['Implied Volatility'].strip('%'))/100,float(puts[np.argmin(np.array([np.abs(x['Strike']-S) for x in puts]))]['Implied Volatility'].strip('%'))/100])
        t=(datetime.datetime.strptime(expiration,"%Y-%m-%d")-datetime.datetime.today().replace(hour=0,minute=0,second=0,microsecond=0)).days
        if t==0:
            t=1
        IV=IV/np.sqrt(365/t)
        sellcall=[[r['Strike'],r['Bid'],self.sell_call_loss(r['Strike'],S,IV)] for r in calls ]
        sellput=[[r['Strike'],r['Bid'],self.sell_put_loss(r['Strike'],S,IV)] for r in puts]
        #print(sellcall)
        #print(sellput)
        lst=[(['-']*3)+['Strike']+[x[0] for x in sellcall]]
        lst=lst+[(['-']*3)+['Premium']+[x[1] for x in sellcall]]
        lst=lst+[(['-']*3)+['Risk']+[x[2] for x in sellcall]]
        lst=lst+[['Strike','Premium','Risk','Sigma']+[np.abs(x[0]-S)/S/IV for x in sellcall]]
        lst=lst+[p+[np.abs((p[0]-S)/S/IV)]+[x[1]+x[2]+p[1]+p[2] for x in sellcall] for p in sellput][::-1]
        df=pd.DataFrame(lst,columns=['Strike','Premium','Risk','Sigma']+[x[0] for x in sellcall])
        return expiration,optiondate,S,IV,df


    def daydiff(to,fr,min=1):
        daydiff=(datetime.datetime.strptime(do,"%Y-%m-%d")-datetime.datetime.strptime(fr,"%Y-%m-%d")).days
        if min is not None and daydiff<min:
            daydiff=min
        return daydiff

    def strangle_eval(self,sym,tradedate='2021-06-15',expiration='2021-06-18',IVmul=2):
        self.dbcursor.execute("select close from stock where symbol='"+sym+"' and date='"+tradedate+"'")
    
        S=self.dbcursor.fetchall()
        if len(S)<1:
            return None
        S=S[0][0]
        self.dbcursor.execute("select low,high from stock where symbol='"+sym+"' and date>='"+tradedate+"' and date<='"+expiration+"'")
        p=np.array(self.dbcursor.fetchall())
        min=np.min(p)
        max=np.max(p)
#       self.dbcursor.execute("select last_price,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='call'")
        self.dbcursor.execute("select bid,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='call'")
        calls=np.asarray(self.dbcursor.fetchall()).transpose()
        if len(calls)<1:
            return None
#       self.dbcursor.execute("select last_price,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='put'")
        self.dbcursor.execute("select bid,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='put'")
        puts=np.asarray(self.dbcursor.fetchall()).transpose()
        calli=np.argmin([np.abs(x-S) for x in calls[1]])
        puti=np.argmin([np.abs(x-S) for x in puts[1]])
        daydiff=(datetime.datetime.strptime(expiration,"%Y-%m-%d")-datetime.datetime.strptime(tradedate,"%Y-%m-%d")).days
        IV=np.average([calls[2][calli],puts[2][puti]])/np.sqrt(365/daydiff)
        lowIV=S*(1-IV*IVmul)
        highIV=S*(1+IV*IVmul)
        calli=np.argmin([np.abs(x-highIV) for x in calls[1]])
        puti=np.argmin([np.abs(x-lowIV) for x in puts[1]])
        premium=calls[0][calli]+puts[0][puti]
        lowBreak =np.min([min-lowIV,0])
        highBreak=np.max([max-highIV,0])    
        return {'stock':S,'stock_min':min,'stock_max':max,'low2Sig':lowIV,'high2Sig':highIV,'premium':premium,'sig':IV*S,'lowBreak':lowBreak,'highBreak':highBreak,'tradeGain':premium+lowBreak-highBreak}

        
    
    def strangle_analyze(self,symlist,tradedate='2021-06-15',expiration='2021-06-18',IVmullist=[2],include_pd=False):
        result=[]
    
        symlist=[s for s in symlist if s != 'AMZN']
        for IVmul in IVmullist:
            ret=[]
            for sym in symlist:
                x=self.strangle_eval(sym,tradedate='2021-06-15',expiration='2021-06-18',IVmul=IVmul)
                if x is not None:
                    x['symbol']=sym
                    ret.append(x)
            df=pd.DataFrame.from_records(ret)
     #       print(df['tradeGain'],df['symbol'])
            val={
                    'SigmaCriteria': IVmul,
                    'TradeDate': tradedate,
                    'Expiration':expiration,
                    'TradeGain':df['tradeGain'].sum(),
                    'Premium': df['premium'].sum(),
                    'StockPrice': df['stock'].sum(),
                    'StrangleCount': df.count()[0],
                    'BreakBracket':  len(df[(df['lowBreak']!=0)| (df['highBreak']!=0)]),
                    'LossTreade' : len(df[df['tradeGain']<0]),
                    'Return': df['tradeGain'].sum()*5/df['stock'].sum(),
                    'ExcerciseRisk': len(df[(df['lowBreak']!=0)| (df['highBreak']!=0)])/len(df['stock'])
                    }
            if include_pd:
                val['pd']=df
            result.append(val)
    
        return result
    
    def fetch_db_stock(self,sym,db='stock', startdate='2021-06-15',enddate='2022-06-18'):
        datefield='date'
        if db=='cboe_stock':
            datefield='tradedate'
        if startdate is None:
            startdate='1900-01-01'
        if enddate is None:
            enddate='2051-01-01'
        result=self.db_fetch_dict("select * from "+db+" where symbol='"+sym+"' and "+datefield+">='"+startdate+"' and "+datefield+"<='"+enddate+"'")
        if db=="cboe_stock":
            for ind in range(len(result)):
                result[ind]['adj_close']=result[ind]['close'];
                result[ind]['volume']=int(result[ind]['volume']);
                result[ind]['date']=result[ind]['tradedate'];
        return  result
    
    def strangle_expected_return(self,sym,tradedate='2021-06-15',expiration='2021-06-18',IVmul=2):
#       self.dbcursor.execute("select close from stock where symbol='"+sym+"' and date='"+tradedate+"'")
    
#       S=self.dbcursor.fetchall()
            
        stock=self.fetch_db_stock(sym,db='cboe_stock',startdate=tradedate,enddate=expiration)
        S=[s['close'] for s in stock if s['date'].strftime("%Y-%m-%d")==tradedate]
        if len(S)<1:
            return None
        S=S[0]
#       self.dbcursor.execute("select low,high from stock where symbol='"+sym+"' and date>='"+tradedate+"' and date<='"+expiration+"'")
#       p=np.array(self.dbcursor.fetchall())
        min=np.min([s['low'] for s in stock])
        max=np.max([s['high'] for s in stock])
#       self.dbcursor.execute("select last_price,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='call'")
        calls=self.fetch_optioncboe(sym,tradedate=tradedate,expiration=expiration,typ="call")
        if len(calls)<1:
            return None
        calls=[
              [x['bid'] for x in calls],
              [x['strike'] for x in calls],
              [x['implied_volatility'] for x in calls]]
        puts=self.fetch_optioncboe(sym,tradedate=tradedate,expiration=expiration,typ="put")
        puts=[
              [x['bid'] for x in puts],
              [x['strike'] for x in puts],
              [x['implied_volatility'] for x in puts]]

#       calls=np.asarray(self.dbcursor.fetchall()).transpose()
#       self.dbcursor.execute("select last_price,strike,implied_volatility from option_yh where symbol='"+sym+"' and tradedate='"+tradedate+"' and expiration='"+expiration+"' and type='put'")
#       puts=np.asarray(self.dbcursor.fetchall()).transpose()
        calli=np.argmin([np.abs(x-S) for x in calls[1]])
        puti=np.argmin([np.abs(x-S) for x in puts[1]])
        daydiff=(datetime.datetime.strptime(expiration,"%Y-%m-%d")-datetime.datetime.strptime(tradedate,"%Y-%m-%d")).days
        IV=np.average([calls[2][calli],puts[2][puti]])/np.sqrt(365/daydiff)

        calli=np.argmin([np.abs(x-(S+IV*IVmul*S)) for x in calls[1]])
        puti=np.argmin([np.abs(x-(S-IV*IVmul*S)) for x in puts[1]])

        sellcall=[calls[1][calli],calls[0][calli],self.sell_call_loss(calls[1][calli],S,IV)]
        sellput =[puts[1][puti],  puts[0][puti],  self.sell_put_loss(puts[1][puti],S,IV)]

#       return {'symbol':sym,'stock':S,'strike_call':sellcall[0],'strike_put':sellput[0],'premium_call':sellcall[1],'premium_put':sellput[1],'loss_call':sellcall[2],'loss_put':sellput[2],'permium':sellcall[1]+sellput[1],'loss':sellcall[2]+sellput[2],'Net':sellcall[1]+sellput[1]+sellcall[2]+sellput[2],'sig_price':IV*S,'callsigma':(sellcall[0]/S-1)/IV,'putsigma':(1-sellput[0]S)/IV }
        return {'symbol':sym,'stock':S,'strike_call':sellcall[0],'strike_put':sellput[0],'premium_call':sellcall[1],'premium_put':sellput[1],'loss_call':sellcall[2],'loss_put':sellput[2],'permium':sellcall[1]+sellput[1],'loss':sellcall[2]+sellput[2],'net':sellcall[1]+sellput[1]+sellcall[2]+sellput[2],'sig_price':IV*S,'callsigma':(sellcall[0]/S-1)/IV,'putsigma':(1-sellput[0]/S)/IV ,'return':(sellcall[1]+sellput[1]+sellcall[2]+sellput[2])*5/S}

    def cal_historical_sigma(self,sym, startDate=None,endDate=None,period=365):
        cond="symbol='"+sym+"' "
        if startDate is not None:
            cond=cond+" and tradedate>='"+startDate+"' "
        if endDate is not None:
            cond=cond+" and dat<='"+endDate+"' "
#       rows=self.fetch_db_stock(sym)
        self.dbcursor.execute("select tradedate,close from cboe_stock where "+cond+" order by tradedate")
        rows=self.dbcursor.fetchall()
#       rows=[[r['date'],r['close']] for r in rows]

        xy= [v for v in [[s,*([x for x in rows if x[0]>s[0]+datetime.timedelta(days=period)][:1])] for s in rows] if len(v)==2]
        r=[np.log(x[1][1]/x[0][1]) for x in xy]
        d=[x[1][0] for x in xy]
        mu,sigma=scipy.stats.norm.fit(r)
        return  sigma,mu,d,r

    def get_symbol_db(self,table='option_yh'):
        self.dbcursor.execute('select distinct symbol from '+table+' order by symbol')
        return [x[0] for x in self.dbcursor.fetchall()]

    # is faction of  365 days
    def optionvalue(self,Strike,Stock,r,t,sig,call=True):
        d1=(np.log(Stock/Strike)+(r+sig*sig/2.)*t)/(sig*np.sqrt(t))
        d2=d1-sig*np.sqrt(t)
#       print(np.log(Stock/Strike))
        Nd1=np.exp(-d1*d1/2)/np.sqrt(2*3.14159265358979)
        Nd2=norm.cdf(d2)
        if call:
            return Stock*norm.cdf(d1)-Strike*np.exp(-r*t)*norm.cdf(d1-sig*np.sqrt(t))
        else:
            return -norm.cdf(-d1)*Stock+norm.cdf(-d2)*Strike*np.exp(-r*t)
    
    # is faction of  365 days
    def solve_option_sig(self,Strike,Stock,r,t,tgt,call=True,xtol=1e-26,ini=0.9):
#       sig=scipy.optimize.fsolve(lambda sig: (self.optionvalue(Strike,Stock,r,t,sig,call=call)-tgt)**2,[ini],xtol=xtol)
#       return sig[0]
        if call:
            sig = mibian.BS([Stock, Strike, r, t*365], callPrice= tgt)/100
        else:
            sig = mibian.BS([Stock, Strike, r, t*365], putPrice= tgt)/100
        return sig.impliedVolatility

    def decode_option_symbol(self,s):
            s=s[-16:]
            return '20'+s[1:3]+'-'+s[3:5]+'-'+s[5:7], ('call' if s[7]=='C' else 'put'),float(s[8:])/1000
    def update_cboe_db_by_file(self,file):
        inf=gzip.open(file)
        data=json.loads(inf.read())
        option=data['data']['options']
        stock=data['data']
        
        tradedate=file[file.rindex('_')+1:file.rindex('.json.gz')]
        tradedate='-'.join([tradedate[:4],tradedate[4:6],tradedate[6:]])
        _expiration,_type,_strike=self.decode_option_symbol(option[0]['option'])
        for n in range(len(option)):
            _expiration,_type,_strike=self.decode_option_symbol(option[n]['option'])
            option[n]['expiration']=_expiration
            option[n]['type']=_type
            option[n]['strike']=_strike
            option[n]['tradedate']=tradedate
            option[n]['close']=stock['close']
            option[n]['symbol']=stock['symbol']
            option[n]['timestamp']=data['timestamp']
        stock['timestamp']=data['timestamp']
        stock['tradedate']=tradedate
        stock['symbol']=data['symbol']
        del stock['options']
        self.insertDictArray('cboe_stock',[stock])
        self.insertDictArray('cboe_option',option)

    def update_cboe_db(self,path):
        flist=glob.glob(path)
        a=[[s[s.rindex('/')+1:s.rindex('_')],'-'.join(*[[x[0:4],x[4:6],x[6:8]] for x in [s[s.rindex('_')+1:]]])] for s in flist]
        d={}
        for x in a:
            if x[0] not in d:
                d[x[0]]=[x[1]]
            else:
                d[x[0]].append(x[1])
        newlist=[]
        for k in d:
            existed=set([s[0].strftime('%Y-%m-%d') for s in np.array(self.db_fetch("select tradedate from cboe_option where symbol='"+k+"'"))])
            s1=set(d[k])
            s1=s1-existed
            newlist=newlist+['/'+k+'_'+x.replace('-','') for x in s1]
        cnt=0;
        for f in [f for f in flist for x in newlist if x in f]:
            try:
                self.update_cboe_db_by_file(f)
                cnt=cnt+1
            except Exception as ex:
                print(f)
                print(str(ex))
                pass
        print("Downloaded %d files" % (cnt))

#   returns number of days up, number of days down, total number of days, net up day divide by total
    def momentum_ma(self,period,sym,db='cboe_stock'):
        if not hasattr(period, '__len__'):
            period=[period]
        nmax=max(period)
        if db=='cboe_stock':
            x=self.db_fetch('select close, tradedate from  cboe_stock where symbol=\''+sym+'\' order by tradedate ')
        else:
            x=self.db_fetch('select close, date from  '+db+' where symbol=\''+sym+'\' order by date ')
        if len(x)==0:
            return None
        x=np.array(x).transpose()
        ret=[]
        for p in period:
            ma=np.array([np.average(np.array(x[0][n-p:n]).astype(float))  for n in range(p,len(x[0]))])
            above=np.array(x[0][p:])-np.array(ma)
            stat=[len([_x for _x in above[-p:] if _x>0 ]),len([_x for _x in above[-p:] if _x<0 ]),len(above[-p:])]
            if stat[2]==0:
                ret.append(None)
                continue
            try:
                mnt=(stat[0]-stat[1])/stat[2]
            except:
                ret.append(None)
            ret.append(stat+[mnt])
        return ret

    def momentum_ma_hist(self,period,sym,db='cboe_stock'):
        if not hasattr(period, '__len__'):
            period=[period]
        nmax=max(period)
        if db=='cboe_stock':
            x=self.db_fetch('select close, tradedate from  cboe_stock where symbol=\''+sym+'\' order by tradedate ')
        else:
            x=self.db_fetch('select close, date from  '+db+' where symbol=\''+sym+'\' order by date ')
        if len(x)==0:
            return None
        x=np.array(x).transpose()
        ret=[]
        xx=x[0]
        ret.append(x[1][nmax:])
        for p in period:
            ma=np.array([np.average(np.array(xx[n-p:n]).astype(float))  for n in range(p,len(xx))])
            above=np.array(xx[p:])-np.array(ma)
            arr=[]
            for n in range(nmax,len(xx)):
                stat=[len([_x for _x in above[n-p:n] if _x>0 ]),len([_x for _x in above[n-p:n] if _x<0 ]),len(above[n-p:n])]
                if stat[2]==0:
                    arr.append(None)
                    continue
                try:
                    mnt=(stat[0]-stat[1])/stat[2]
                except:
                    arr.append(None)
                arr.append(mnt)
            ret.append(arr)
        return ret
    #return maxpain strike price, index of the price,and [strike,accumulative cost] for call, and put
    # it returns: {"trade":[{"entrydate":,"entryprice":,"exitdate":,"exitprice":,"gain":,"entryindex","exitindex":}],
    #              "data": {"date":,"close","mask","indicator"}}
    # gain is ratio
    def momentum_ma_signal(self,sym,span=500,period=[5,22,66],db='stock',criteria=lambda x: x[2]>0.75 and x[3]>0.75):
        ret=self.momentum_ma_hist(period,sym,db=db)
        r=np.array(ret).transpose();
#        ret.append([(1 if ret[2][x]>criteria and ret[3][x]>criteria else 0) for x in range(len(ret[0]))])
        ret.append([(1 if criteria(r[x]) else 0) for x in range(len(ret[0]))])
        close=self.db_fetch('select date, close from '+db+' where symbol="'+sym+'" order by date')
        close_fullrange=[x[1] for x in close]
        close=close[-span:]
        mask=ret[4][-span:]
        close=np.array(close).transpose()
        up=[x for x in range(1,len(mask)) if  mask[x-1]==0 and mask[x]==1]
        dn=[x for x in range(1,len(mask)) if  mask[x-1]==1 and mask[x]==0]
        print(up)
        print(dn)
        if dn[0]<up[0]:
            dn=dn[1:]
        if len(dn)==0 or up[-1]>dn[-1]:
            dn.append(len(mask)-1)
        trade=[]    
        for x in range(len(up)):
            trade.append({
                "entrydate":   close[0][up[x]],
                "entryprice":  close[1][up[x]],
                "exitdate":    close[0][dn[x]],
                "exitprice":   close[1][dn[x]],
                "gain":       (close[1][dn[x]]-close[1][up[x]])/close[1][up[x]],
                "entryindex": up[x]+len(ret[0])-len(close[0]),
                "exitindex":  dn[x]+len(ret[0])-len(close[0])
            })
        return {"trade": trade,"data": {
                    "date":     ret[0],
                    "close":    close_fullrange[-len(ret[0]):],
                    "mask":     ret[4][-len(ret[0]):],
                    "indicator": ret[1:-1]
                }
            }



    def maxpain(self,sym,expiration,tradedate):
        a=self.db_fetch('select type,strike, open_interest,`option` from cboe_option where symbol="'+sym+'" and expiration="'+expiration+'" and tradedate="'+tradedate+'" order by type,strike')
        maxpain_call=np.array([[x[1],x[2]] for x in a if x[0]=='call']).transpose()
        maxpain_put=np.array([[x[1],x[2]] for x in a if x[0]=='put']).transpose()
#       strikelist=list(set(maxpain_call[0]).union(set(maxpain_put[0])))
#       strikelist.sort()
#       strikelist=np.array(strikelist)
#       print(strikelist)
#       maxpain_strike=[]
#       maxpain_call_dollars=[]
#       maxpain_put_dollars=[]
#       min_dollars=0
#       max_pain_strike=None
#       for working_strike in strikelist:
#           put_dollars=0
#           call_dollars=0
#           for strike in strikelist:
#               price_delta=working_strike-strike
#               if price_delta>0 and strike in maxpain_call[0]:
#                   call_dollars+=price_delta*maxpain_call[1][np.where(maxpain_call[0]==strike)]
#               elif price_delta<0:
#                   put_dollars+=price_delta*maxpain_put[1][np.where(maxpain_put[0]==strike)]*-1
#           maxpain_strike.append(working_strike)
#           maxpain_call_dollars.append(call_dollars)
#           maxpain_put_dollars.append(put_dollars)
#           total_strike_dollars=call_dollars+put_dollars

#           if max_pain_strike is None or (total_strike_dollars<min_dollars):
#               min_dollars=total_strike_dollars
#               max_pain_strike=working_strike
#       return max_pain_strike,maxpain_call_dollars,maxpain_put_dollars

        maxpain_call=np.array([ [maxpain_call[0][x],  np.dot(-maxpain_call[0][:x+1]+np.array([maxpain_call[0][x]]*(x+1)),maxpain_call[1][:x+1])]   for x in range(len(maxpain_call[0]))]).transpose()
        maxpain_put =np.array([ [maxpain_put[0][x],  np.dot(maxpain_put[0][x:]-np.array([maxpain_put[0][x]]*len(maxpain_put[0][x:])),maxpain_put[1][x:])]   for x in range(len(maxpain_put[0]))]).transpose()

        ind=np.argmin(np.abs(maxpain_call[1]+maxpain_put[1]))
        maxpainprice=maxpain_call[0][ind]
        return maxpainprice,ind,maxpain_call*100,maxpain_put*100

    #usage:
    #   model_3 = get_natural_cubic_spline_model(x, y, minval=min(x), maxval=max(x), n_knots=3)
    #   y_est_3 = model_3.predict(x)
    def get_natural_cubic_spline_model(self,x, y, minval=None, maxval=None, n_knots=None, knots=None):
        """
        Get a natural cubic spline model for the data.

        For the knots, give (a) `knots` (as an array) or (b) minval, maxval and n_knots.

        If the knots are not directly specified, the resulting knots are equally
        space within the *interior* of (max, min).  That is, the endpoints are
        *not* included as knots.

        Parameters
        ----------
        x: np.array of float
            The input data
        y: np.array of float
            The outpur data
        minval: float
            Minimum of interval containing the knots.
        maxval: float
            Maximum of the interval containing the knots.
        n_knots: positive integer
            The number of knots to create.
        knots: array or list of floats
            The knots.

        Returns
        --------
        model: a model object
            The returned model will have following method:
            - predict(x):
                x is a numpy array. This will return the predicted y-values.
        """

        if knots:
            spline = self.NaturalCubicSpline(knots=knots)
        else:
            spline = self.NaturalCubicSpline(max=maxval, min=minval, n_knots=n_knots)

        p = Pipeline([
            ('nat_cubic', spline),
            ('regression', LinearRegression(fit_intercept=True))
        ])

        p.fit(x, y)

        return p


    class AbstractSpline(BaseEstimator, TransformerMixin):
        """Base class for all spline basis expansions."""

        def __init__(self, max=None, min=None, n_knots=None, n_params=None, knots=None):
            if knots is None:
                if not n_knots:
                    n_knots = self._compute_n_knots(n_params)
                knots = np.linspace(min, max, num=(n_knots + 2))[1:-1]
                max, min = np.max(knots), np.min(knots)
            self.knots = np.asarray(knots)

        @property
        def n_knots(self):
            return len(self.knots)

        def fit(self, *args, **kwargs):
            return self


    class NaturalCubicSpline(AbstractSpline):
        """Apply a natural cubic basis expansion to an array.
        The features created with this basis expansion can be used to fit a
        piecewise cubic function under the constraint that the fitted curve is
        linear *outside* the range of the knots..  The fitted curve is continuously
        differentiable to the second order at all of the knots.
        This transformer can be created in two ways:
          - By specifying the maximum, minimum, and number of knots.
          - By specifying the cutpoints directly.

        If the knots are not directly specified, the resulting knots are equally
        space within the *interior* of (max, min).  That is, the endpoints are
        *not* included as knots.
        Parameters
        ----------
        min: float
            Minimum of interval containing the knots.
        max: float
            Maximum of the interval containing the knots.
        n_knots: positive integer
            The number of knots to create.
        knots: array or list of floats
            The knots.
        """

        def _compute_n_knots(self, n_params):
            return n_params

        @property
        def n_params(self):
            return self.n_knots - 1

        def transform(self, X, **transform_params):
            X_spl = self._transform_array(X)
            if isinstance(X, pd.Series):
                col_names = self._make_names(X)
                X_spl = pd.DataFrame(X_spl, columns=col_names, index=X.index)
            return X_spl

        def _make_names(self, X):
            first_name = "{}_spline_linear".format(X.name)
            rest_names = ["{}_spline_{}".format(X.name, idx)
                          for idx in range(self.n_knots - 2)]
            return [first_name] + rest_names

        def _transform_array(self, X, **transform_params):
            X = X.squeeze()
            try:
                X_spl = np.zeros((X.shape[0], self.n_knots - 1))
            except IndexError: # For arrays with only one element
                X_spl = np.zeros((1, self.n_knots - 1))
            X_spl[:, 0] = X.squeeze()

            def d(knot_idx, x):
                def ppart(t): return np.maximum(0, t)

                def cube(t): return t*t*t
                numerator = (cube(ppart(x - self.knots[knot_idx]))
                             - cube(ppart(x - self.knots[self.n_knots - 1])))
                denominator = self.knots[self.n_knots - 1] - self.knots[knot_idx]
                return numerator / denominator

            for i in range(0, self.n_knots - 2):
                X_spl[:, i+1] = (d(i, X) - d(self.n_knots - 2, X)).squeeze()
            return X_spl

    # it returns function and actual data
    def get_smooth_sigma_func(self,sym,tradedate,expiration,call=True,span=1.2,n_knots=9):
        d=self.db_fetch('select (ask+ask)/2 as price, close,strike,volume,greatest(expiration-tradedate,1) from cboe_option where symbol=\''+sym+'\' and tradedate=\''+tradedate+'\' and expiration=\''+expiration+'\' and type=\''+('call' if call else 'put')+ '\' and bid<>0 and ask<>0 and volume>100 and strike<close*'+str(span)+' and strike>close*(2-'+str(span)+')  order by strike')

        if call:
            sig = np.array([c.impliedVolatility for c in [mibian.BS([x[1], x[2], 5, x[4]], callPrice= x[0]) for x in d ]])/100
        else:
            sig = np.array([p.impliedVolatility for p in [mibian.BS([x[1], x[2], 5, x[4]], putPrice= x[0]) for x in d ]])/100
        d=[x[2] for x in d]
        d=np.array(d)
        sig=np.array(sig)
        _min=np.min(d)
        _max=np.max(d)
        if n_knots is not None:
            return self.get_natural_cubic_spline_model(d, sig, minval=_min*0.8, maxval=_max*1.2, n_knots=n_knots).predict,d,sig,n_knots
        
        dx=np.arange(_min,_max,0.1)
        for n in range(len(d),0,-1):
            f=self.get_natural_cubic_spline_model(d, sig, minval=_min, maxval=_max, n_knots=n).predict
            v=np.array([f(x) for x in dx])
            v=v[1:]-v[:-1]
            zero=[x for x in range(len(v)-1)  if ((v[x]<=0 and v[x+1]>0) or (v[x]>=0 and v[x+1]<0))]
#           print(n)
#           print(v)
#           print(zero)
            if len(zero)==1:
                return f,d,sig,n
        return None,None,None,None

    def dl_stock_1min(self,sym,db='stock_rt'):
        key="95YGMU28VCQR59OL"
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+sym+'&outputsize=full&interval=1min&apikey='+key
        r = requests.get(url)
        data = r.json()
        data=data['Time Series (1min)']
        d=[[sym,k,data[k]['1. open'],data[k]['2. high'],data[k]['3. low'],data[k]['4. close'],data[k]['5. volume']] for k in data]
        self.insert2Field(db,'symbol,time,open,high,low,close,volume','%s %s %s %s %s %s',d);


