

from   scipy.stats import norm
#import finpie.price_data
#import finpie
#import yahoo_fin.options as ops
import datetime
import numpy as np
import mysql.connector
#import pandas.util.testing as tm
#import pandas as pd
#from   yahoo_fin import stock_info as si
#import robin_stocks.robinhood as rh
import scipy
import scipy.optimize
from   scipy.stats import norm
#import gzip
#import json
#import glob
import os
from datetime import timedelta
import  datetime 
import matplotlib.dates as mdates
import etoption 

from IPython.core.display import display, HTML
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import importlib
import stockquotes
#import ol_yhoption
import mibian

class uioption:
    db=None
    dbcursor=None
    et=None

    def __init__(self,database='finance',user='root',passwd='Password.11',et=None):
        display(HTML("<style>.container { width:100% !important; }</style>"))
        plt.rcParams['figure.figsize'] = [20, 8]
        plt.rcParams['figure.dpi'] = 100

        if et is not None:
            self.et=et
        else:
            self.et=etoption.etoption(database=database,user=user,passwd=passwd)
            self.et.db_init(database=database,user=user,passwd=passwd)
        db=self.et.db
        dbcursor=self.et.dbcursor
        
    def getdbcursor(self):
        return self.dbcursor


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
    
    def plot_iv_vs_expiration_dbd(self,sym,period=7,tradedate=None,strike=None):
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax3=ax.twinx()
        if tradedate is None:
            tradedate=self.et.fetch_near_tradedate(sym)
        elif type(tradedate) ==str:
            tradedate=datetime.datetime.strptime(tradedate,'%Y-%m-%d')
        if strike is None:
            strike=self.et.fetch_near_strike(sym)
        explist_c=self.et.db_fetch_dict("select close,type,tradedate,  expiration, greatest(1,datediff(expiration,tradedate)) as days,strike,(ask+bid)/2 as price from cboe_option where symbol='"+sym+"' and expiration > curdate() and bid<>0 and ask<>0 and iv<>0 and tradedate='"+str(tradedate)+"' and type='call' and strike="+str(strike)+" order by strike,expiration asc")
        explist_p=self.et.db_fetch_dict("select close,type,tradedate,  expiration, greatest(1,datediff(expiration,tradedate)) as days,strike,(ask+bid)/2 as price from cboe_option where symbol='"+sym+"' and expiration > curdate() and bid<>0 and ask<>0 and iv<>0 and tradedate='"+str(tradedate)+"' and type='put' and strike="+str(strike)+" order by strike,expiration asc")
        myiv_c=np.array([self.et.solve_option_sig(x['strike'],x['close'],0.05,x['days']/365,x['price']) for x in explist_c])
        myiv_p=np.array([self.et.solve_option_sig(x['strike'],x['close'],0.05,x['days']/365,x['price'],call=False) for x in explist_p])
        ax.plot([x['expiration'] for x in explist_c], myiv_c*100,label='call',marker='o',color='orange',zorder=2)
        ax.plot([x['expiration'] for x in explist_p], myiv_p*100,label='put',marker='o',color='blue',zorder=2)
        myiv_c_period=np.multiply(myiv_c,np.array([np.sqrt(x['days']/365) for x in explist_c]))
        myiv_p_period=np.multiply(myiv_p,np.array([np.sqrt(x['days']/365) for x in explist_p]))
        myiv_c_period_chg=np.multiply(myiv_c,np.array([np.sqrt(x['days']/365)-np.sqrt((x['days']-period if x['days']>=period else 0)/ 365.0) for x in explist_c]))
        myiv_p_period_chg=np.multiply(myiv_p,np.array([np.sqrt(x['days']/365)-np.sqrt((x['days']-period if x['days']>=period else 0)/ 365.0) for x in explist_p]))
        ax.legend(loc=0)
        ax.set_ylabel("Implied Anual Volatility")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax2.plot([x['expiration']-timedelta(days=1) for x in explist_c], myiv_c_period*100,':',label='call',color='orange',alpha=0.5,zorder=1)
        ax2.plot([x['expiration']+timedelta(days=2) for x in explist_p], myiv_p_period*100,':',label='put',color='blue',alpha=0.5,zorder=1)
        ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax2.set_ylabel("Implied Period Volatility (Dash line)")
        ax2.spines['top'].set_visible(False)
        ax2.spines['bottom'].set_visible(False)
        ax2.spines['left'].set_visible(False)
        ax2.spines['right'].set_position(('outward', 60))
        ax3.bar([x['expiration']-timedelta(days=1) for x in explist_c], myiv_c_period_chg*100,label='call',color='orange',alpha=0.5,width=3,zorder=1)
        ax3.bar([x['expiration']+timedelta(days=2) for x in explist_p], myiv_p_period_chg*100,label='put',color='blue',alpha=0.5,width=3,zorder=1)
        #plt.plot([x['expiration'] for x in explist_c],[x['iv'] for x in explist_c], label='call')
        ax3.set_ylim(bottom=0)
        ax3.set_ylabel("Implied Period Volatility Reduction in "+str(period)+" Days (bar chart)")
        ax3.yaxis.set_major_formatter(mtick.PercentFormatter())
    
        ax.set_title("Implied Volatility for different Expiration\n\n Symbol: "+sym+" Stock price: "+str(explist_c[0]['close'])+" Strike Price: "+str(strike)+" on date: "+str(explist_c[0]['tradedate']),y=0.5)
        plt.show() 
    
        stock=explist_c[0]['close']
        ones=np.array([1]*len(myiv_c_period))
        price_up=(ones+myiv_c_period)*stock
        price_dn=(ones-myiv_p_period)*stock
        plt.fill_between([tradedate]+[x['expiration'] for x in explist_c], [stock]+price_up.tolist(),[stock]+price_dn.tolist())
        plt.grid()
        plt.title('One Sigma Stock Price for '+sym)
        plt.show()

    def plot_iv_vs_strike_dbd(self,sym,period=7,tradedate=None,expiration=None):
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        if tradedate is None:
            tradedate=self.et.fetch_near_tradedate(sym)
        elif type(tradedate) ==str:
            tradedate=datetime.datetime.strptime(tradedate,'%Y-%m-%d')
        if expiration is None:
            expiration=self.et.fetch_near_expiration(sym)
        elif type(tradedate) ==str:
            expiration=datetime.datetime.strptime(expiration,'%Y-%m-%d')
            
        explist_c=self.et.db_fetch_dict("select close,type,tradedate,  strike, greatest(1,datediff(expiration,tradedate)) as days,(bid+ask)/2 as price from cboe_option where symbol='"+sym+"' and expiration = '"+str(expiration)+"' and bid<>0 and ask<>0 and tradedate='"+str(tradedate)+"' and type='call'  and volume <>0 order by strike asc")
        explist_p=self.et.db_fetch_dict("select close,type,tradedate,  strike, greatest(1,datediff(expiration,tradedate)) as days,(bid+ask)/2 as price from cboe_option where symbol='"+sym+"' and expiration = '"+str(expiration)+"' and bid<>0 and ask<>0 and tradedate='"+str(tradedate)+"' and type='put'   and volume <>0 order by strike asc")
        myiv_c=np.array([self.et.solve_option_sig(x['strike'],x['close'],0.05,x['days']/365,x['price']) for x in explist_c])
        myiv_p=np.array([self.et.solve_option_sig(x['strike'],x['close'],0.05,x['days']/365,x['price'],call=False) for x in explist_p])
        ax.scatter([x['strike'] for x in explist_c], myiv_c*100,label='call',marker='o',color='orange',zorder=2)
        ax.scatter([x['strike'] for x in explist_p], myiv_p*100,label='put',marker='o',color='blue',zorder=2)
        days=explist_c[0]['days']
        myiv_c_period=myiv_c*np.sqrt(days/365)
        myiv_p_period=myiv_p*np.sqrt(days/365)
        ax.legend(loc=0)
        ax.set_ylabel("Implied Anual Volatility")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax2.scatter([x['strike'] for x in explist_c], myiv_c_period*100,label='call',color='orange',zorder=1)
        ax2.scatter([x['strike'] for x in explist_p], myiv_p_period*100,label='put',color='blue',zorder=1)
        ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
        ax2.set_ylabel("Implied Period Volatility")
        ax2.set_ylim(bottom=0)
        cind=np.argmin(np.array([np.abs(x['strike']-x['close']) for x in explist_c]))
        pind=np.argmin(np.array([np.abs(x['strike']-x['close']) for x in explist_p]))
        ax.set_title("Implied Volatility for Different Strike price\n\n Symbol: "+sym+" Stock price: "+str(explist_c[0]['close'])+" on date: "+str(explist_c[0]['tradedate'])+ " Expiration: "+str(expiration),y=0.8)
    #    ax.text(datetime.strftime(explist_c[cind]['tradedate'],'%Y-%m-%d'),myiv_c[cind],"ATM Call "+str(days)+" day volatility: "+str(myiv_c[cind])+" \$"+str(myiv_c[cind]*explist_c[0]['close']))
        ax.text(0.39,0.7,("ATM  %d day call volatility: %5.2f%%  \$%-5.2f, put volatility: %5.2f%%  \$%-5.2f" % (days,myiv_c_period[cind]*100,myiv_c_period[cind]*explist_c[0]['close'],myiv_p_period[cind]*100,myiv_p_period[cind]*explist_p[0]['close']))
            ,transform=ax.transAxes)
        plt.show() 


    def plt_option_run(self,tx,span=1.15):
        f=[
          [ol_yhoption.ol_yhoption.curve(x[0],x[1],x[2],ol_yhoption.get_premium(x[0],x[2],expiration,option_data['put']  if x[1]=='p' else option_data['call']) if x[3] is None else x[3])   for x in _tx]  for _tx in tx
        ]
        colorlist=["#FFFF00", "#FF00FF", "#FF0000", "#C0C0C0", "#808080", "#808000", "#800080", "#800000", "#00FFFF", "#00FF00", "#008080", "#008000", "#0000FF", "#000080", "#000000"]
        xx=[x[2] for _tx in tx for x in _tx]
        xmin=min(xx)
        xmax=max(xx)
        print(xx)
        xlist=np.arange(*[[np.min(_x[0])*(1-span),np.max(_x[0])*span,0.1] for _x in [[x[2] for _tx in tx for x in _tx]]][0])
        xlist=np.arange(xmin*(2-span),xmax*span,0.1)
        for n in range(1,len(f)+1):
            plt.plot(xlist,[np.sum([ff(x) for _f in f[:n] for ff in _f]) for x in xlist],color=colorlist[n%len(colorlist)],label="Group "+str(n)+' '+str(tx[n-1]))
        plt.legend(loc="upper left")
        plt.grid()
        plt.show()

    def plt_volume_oi_atmiv(self,sym,expiration):
        a=self.et.db_fetch_dict("select type,tradedate, sum(volume) as volume ,sum(open_interest) as oi from cboe_option where symbol='"+sym+"'  and expiration='"+expiration+"' group by tradedate, type")
        plt.rcParams['figure.figsize'] = [12, 8]
        plt.rcParams['figure.dpi'] = 100
        c=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='call']
        p=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='put']
        c=np.array(c).transpose()
        p=np.array(p).transpose()
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax3=ax.twinx()
        ax3.set_ylabel("Open Interest ")
        ax3.spines['right'].set_position(('outward', 60))

        lg=[None]*6
        lg[0],=ax.plot(c[0],c[1],':d',label='call volume', color='blue')
        lg[1],=ax.plot(p[0],p[1],':d',label='put volume', color='orange')
         
        ax.set_ylabel("Volume")
        csig=[self.et.fetch_atm_iv(sym,x.strftime('%Y-%m-%d'),expiration) for x in c[0]]
        psig=[self.et.fetch_atm_iv(sym,x.strftime('%Y-%m-%d'),expiration) for x in p[0]]
        expire=datetime.datetime.strptime(expiration,'%Y-%m-%d').date()
        cdaydiff=[(expire-x).days for x in c[0]]
#       csig=np.array(csig)/np.array(cdaydiff)
        lg[2],=ax2.plot(c[0],csig,'--d',label='Call Sigma',linewidth=4,color='blue',alpha=0.5)
        pdaydiff=[(expire-x).days for x in p[0]]
#       psig=np.array(psig)/np.array(pdaydiff)
        lg[3],=ax2.plot(p[0],psig,'--d',label='put Sigma',linewidth=4,color='orange',alpha=0.5)
        ax2.set_ylabel("Sigma")
        ax3.set_ylabel("Open Interest")
        ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1))
        print(c[2])
        print(p[2])
#       lg[4],=ax3.plot(c[0][1:],c[2][1:]-c[2][:-1],':d',label='Call Open Interest Change',color='blue')
#       lg[5],=ax3.plot(c[0][1:],p[2][1:]-p[2][:-1],':d',label='Put Open Interest Change',color='orange')
        lg[4],=ax3.plot(c[0],c[2],'-o',label='Call Open Interest Change',color='blue')
        lg[5],=ax3.plot(c[0],p[2],'-o',label='Put Open Interest Change',color='orange')
        plt.legend(loc='upper left',handles=lg)
        plt.title(sym+" Option volume and ATM Sigma, expiratin: "+str(expiration))
        plt.subplots_adjust(right=0.85 )

    def plt_volume_oi(self,sym):
        a=self.et.db_fetch_dict("select type,tradedate, sum(volume) as volume ,sum(open_interest) as oi from cboe_option where symbol='"+sym+"'  group by tradedate, type")
        plt.rcParams['figure.figsize'] = [12, 8]
        plt.rcParams['figure.dpi'] = 100
        c=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='call']
        p=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='put']
        c=np.array(c).transpose()
        p=np.array(p).transpose()
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax3=ax.twinx()
        ax3.spines['right'].set_position(('outward', 60))

        lg=[None]*6
        lg[0],=ax.plot(c[0],c[1],':d',label='call volume', color='blue')
        lg[1],=ax.plot(p[0],p[1],':d',label='put volume', color='orange')
         
        ax.set_ylabel("Volume")

        ax2.set_ylabel("Open Interest")
        print(c[2])
        print(p[2])
#       lg[4],=ax3.plot(c[0][1:],c[2][1:]-c[2][:-1],':d',label='Call Open Interest Change',color='blue')
#       lg[5],=ax3.plot(c[0][1:],p[2][1:]-p[2][:-1],':d',label='Put Open Interest Change',color='orange')
        lg[2],=ax2.plot(c[0],c[2],'--d',label='Call Open Interest',color='blue')
        lg[3],=ax2.plot(c[0],p[2],'--d',label='Put Open Interest',color='orange')

        lg[4],=ax3.plot(c[0],p[1]/(c[1]+1),'-o',label='Volume Put-to-CAll Ratio',color='green')
        lg[5],=ax3.plot(c[0],p[2]/(c[2]+1),'-o',label='Open Interest Put-to-CAll Ratio',color='purple')
        ax2.set_ylabel("Open Interst")
        ax3.set_ylabel("Put to Call Ratio")
        ax3.set_ylim(ymin=0)
        plt.legend(loc='upper left',handles=lg)
        plt.title(sym+" Option volume and Open Interest for "+sym)
        plt.subplots_adjust(right=0.85 )

    def plt_oi(self,sym,expiration):
        a=self.et.db_fetch_dict("select type,tradedate, open_interest,strike as oi from cboe_option where symbol='"+sym+"'  and expiration='"+expiration+"' group by type asc,strike asc,tradedate desc")
        plt.rcParams['figure.figsize'] = [12, 8]
        plt.rcParams['figure.dpi'] = 100
        c=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='call']
        p=[[x['tradedate'],x['volume'],x['oi']] for x  in a if x['type']=='put']
        c=np.array(c).transpose()
        p=np.array(p).transpose()
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax3=ax.twinx()
        ax3.set_ylabel("Open Interest ")
        ax3.spines['right'].set_position(('outward', 60))

        lg=[None]*6
        lg[0],=ax.plot(c[0],c[1],'-o',label='call volume', color='blue')
        lg[1],=ax.plot(p[0],p[1],'-o',label='put volume', color='orange')
         
        ax.set_ylabel("Volume")
        csig=[self.et.fetch_atm_iv(sym,x.strftime('%Y-%m-%d'),expiration) for x in c[0]]
        psig=[self.et.fetch_atm_iv(sym,x.strftime('%Y-%m-%d'),expiration) for x in p[0]]
        expire=datetime.datetime.strptime(expiration,'%Y-%m-%d').date()
        cdaydiff=[(expire-x).days for x in c[0]]
        csig=np.array(csig)/np.array(cdaydiff)
        lg[2],=ax2.plot(c[0],csig,'--d',label='Call Sigma',color='blue',alpha=0.5)
        pdaydiff=[(expire-x).days for x in p[0]]
        psig=np.array(psig)/np.array(pdaydiff)
        lg[3],=ax2.plot(p[0],psig,'--d',label='put Sigma',color='orange',alpha=0.5)
        ax2.set_ylabel("Sigma")
        ax3.set_ylabel("Change of Net Open Interest")
        ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1))
        lg[4],=ax3.plot(c[0][1:],c[2][1:]-c[2][:-1],':d',label='Call Open Interest Change',color='blue')
        lg[5],=ax3.plot(c[0][1:],p[2][1:]-p[2][:-1],':d',label='Put Open Interest Change',color='orange')
        plt.legend(loc='upper left',handles=lg)
        plt.title(sym+" Option volume, expiratin: "+str(expiration))
        plt.subplots_adjust(right=0.85 )

    def plt_oi_profile(self,sym,expirationlist=None,period=5,span=1.5,width=None):
        if expirationlist is None:
            expirationlist=[self.et.fetch_near_expiration(sym)]

        colorlist=[ "#FF0000",  "#00FF00", "#0000FF", "#808000", "#800080",  "#00FFFF", "#00FF00","#FF00FF", "#008080", "#008000", "#800000","#0000FF", "#000080"]
        color_i=0
        fig,ax=plt.subplots()
        for expiration in expirationlist:
            a=self.et.db_fetch_dict("select type,tradedate, open_interest as oi,strike ,close from cboe_option where symbol='"+sym+"'  and expiration='"+expiration+"' order by type asc,strike asc,tradedate ")
            tdate=[x[0] for x in self.et.db_fetch("select distinct tradedate from cboe_option where symbol='"+sym+"' order by tradedate")]
            tdate=tdate[-period:]
            
#           close=[[_x/span,_x*span] for _x in [np.average(np.array([x['close'] for x in a]))]][0]
            close=[a[-1]['close']*(2-span),a[-1]['close']*span]
            a=[x for x in a if x['strike']>=close[0] and x['strike']<=close[1] and x['tradedate'] in tdate]
            
            oi_c=[[] for x in range(len(tdate))]
            strike_c=[[] for x in range(len(tdate))] 
            oi_p=[[] for x in range(len(tdate))]
            strike_p=[[] for x in range(len(tdate))]
            b_c={}
            b_p={}
            for x in a:
                if x['type'] == 'call':            
                    if not x['strike'] in b_c.keys():
                        b_c[x['strike']]=[]
                    b_c[x['strike']].append([x['tradedate'],x['oi']])
                else:
                    if not x['strike'] in b_p.keys():
                        b_p[x['strike']]=[]
                    b_p[x['strike']].append([x['tradedate'],-x['oi']])
            for k in b_c.keys():
                b_c[k]=np.array(b_c[k]).transpose()
#               print(k)
#               print(b_c[k])
            for k in b_p.keys():
                b_p[k]=np.array(b_p[k]).transpose()
#               print(k)
#               print(b_p[k])
                

                
            

            if width is None:
                strike_ordered=np.array(b_c.keys())
                np.sort(strike_ordered)
                dist=np.average(strike_ordered[1:]-strike_ordered[:-1])
                width=(dist)/(len(tdate)+1)
            
#            for n in range(len(tdate)):
#                color='red' if n==0 else ('blue' if n==len(tdate)-1 else 'grey')
#                if n==0:
#                    plt.bar(np.array(strike_c[n])+width*n,oi_c[n],width=width,color=colorlist[color_i%len(colorlist)],alpha=0.5,label='Expiration '+str(expiration))
#                else:
#                    plt.bar(np.array(strike_c[n])+width*n,oi_c[n],width=width,color=colorlist[color_i%len(colorlist)],alpha=0.5)
#                plt.bar(np.array(strike_p[n])+width*n,oi_p[n],width=width,color=colorlist[color_i%len(colorlist)],alpha=0.5)
#            color_i=color_i+1

            noLabel=True
            for n in b_c:
#               print("in B-C:"+str(n))
                x=np.arange(0,len(b_c[n][0]))
                x=x*width+n
#               print(x)
#               print(b_c[n][1])
                if noLabel:
                    plt.plot(x,b_c[n][1],marker='o',markersize=2,color=colorlist[color_i%len(colorlist)],alpha=0.8,label='Expiration '+str(expiration))
                    noLabel=False
                else:
                    plt.plot(x,b_c[n][1],marker='o',markersize=2,color=colorlist[color_i%len(colorlist)],alpha=0.8)
                plt.plot(x,b_p[n][1],marker='o',markersize=2,color=colorlist[color_i%len(colorlist)],alpha=0.8)
            color_i=color_i+1

        ax.set_ylabel("Put <=                            Open Interest                         => Call")
        plt.legend(loc="upper left")
        plt.title("Open Interest for %s ($%-0.2f) trade day:%s - %s" % (sym,a[-1]['close'],tdate[-period],tdate[-1]),y=0.9)
        plt.grid()
        plt.show()

    def plt_iv_profile(self,sym,expiration=None,period=5,span=1.5,width=None):
        if expiration is None:
            expiration=self.et.fetch_near_expiration(sym)
        a=self.et.db_fetch_dict("select type,tradedate, iv ,strike ,close from cboe_option where symbol='"+sym+"'  and expiration='"+expiration+"' order by type asc,strike asc,tradedate desc")
        tdate=[x[0] for x in self.et.db_fetch("select distinct tradedate from cboe_option where symbol='"+sym+"' order by tradedate")]
        tdate=tdate[-period:]
        
        close=[[_x/span,_x*span] for _x in [np.average(np.array([x['close'] for x in a]))]][0]
        a=[x for x in a if x['strike']>=close[0] and x['strike']<=close[1]]
        
        colorlist=[ "#FF00FF",  "#C0C0C0", "#808080", "#808000", "#800080",  "#00FFFF", "#00FF00","#FF0000", "#008080", "#008000", "#800000","#0000FF", "#000080"]
        colorlist=['grey']
        oi_c=[[] for x in range(len(tdate))]
        strike_c=[[] for x in range(len(tdate))] 
        oi_p=[[] for x in range(len(tdate))]
        strike_p=[[] for x in range(len(tdate))]
        for x in a:
            if x['tradedate'] in tdate :
                if x['type'] == 'call':            
                    oi_c[tdate.index(x['tradedate'])].append(x['iv'])
                    strike_c[tdate.index(x['tradedate'])].append(x['strike'])
                else:
                    oi_p[tdate.index(x['tradedate'])].append(-x['iv'])
                    strike_p[tdate.index(x['tradedate'])].append(x['strike'])  
        strike_set=set(strike_c[0]).union(set(strike_p[0]))
        strike_set=np.array(list(strike_set))
        strike_set=np.sort(strike_set)
        if width is None:
            dist=np.average(np.array(strike_set[1:])-np.array(strike_set[:-1]))
            width=(dist)/(len(tdate)+1)
        
        for n in range(len(tdate)):
            color='red' if n==0 else ('blue' if n==len(tdate)-1 else 'grey')
            plt.bar(np.array(strike_c[n])+width*n,oi_c[n],width=width,color=color,alpha=0.5)
            plt.bar(np.array(strike_p[n])+width*n,oi_p[n],width=width,color=color,alpha=0.5)
        plt.title("Implied Volatility for %s trade day:%s - %s expiration: %s" % (sym,tdate[-period],tdate[-1],expiration),y=0.9)
        plt.grid()
        plt.show()
    
    def plt_sector(self,span=None):
        sectorlist= {
                  'XLK'         :'Technology',
                  'SMH'         :'Semiconductor',
                  'IBB'         :'Biotechnology',
                  'XLV'         :'Health Care',
                  'XLY'         :'Consumer Discretionary',
                  'XRT'         :'Retail',
                  'IYR'         :'Real Estate',
                  'XHB'         :'Home Builders',
                  'XLF'         :'Financial',
                  'KBE'         :'Bank',
                  'XLI'         :'Industrial',
                  'XLP'         :'Consumer Staples',
                  'XME'         :'Metals',
                  'XLB'         :'Materials',
                  'GDX'         :'GoldMiners',
                  'XOP'         :'Oil & Gas',
                  'XLE'         :'Energy',
                  'XLU'         :'Utilities',
                }
        if span is not None:
            lim=' limit '+str(span)
        else:
            lim=''
        colorlist=['Black','Brown','Red','Orange','Yellow','Green','Blue','Violet','Grey','Purple']
        li=0
        ci=0
        nplt=int(np.ceil(len(sectorlist)/len(colorlist)))
        fig,ax=plt.subplots(nplt)
        for sym in sectorlist.keys():
            a=self.et.db_fetch('select tradedate,close from cboe_stock where symbol=\''+sym+'\' order by tradedate desc'+lim)
            a=[list(x) for x in zip(*a)]
            a[1]=np.array(a[1])
            a[1]=a[1]/np.average(a[1][0])-1
            if ci==0:
                ax[li].yaxis.set_major_formatter(mtick.PercentFormatter(1))
                ax[li].set_title("Sector Trends")
            ax[li].plot(a[0],a[1],color=colorlist[ci],label=sectorlist[sym],marker='o')
            ci=ci+1
            if ci>=len(colorlist):
                ci=0
                li=li+1
            ax[li].legend(loc='upper left')
        #plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(1))
        plt.grid()
        return fig

    def plt_vol_sig_trend(self,sym,startdate='2021-07-01',enddate=datetime.date.today().strftime('%Y-%m-%d'),areadim=[20,20],cnt=5):
        d=self.et.db_fetch_dict('select tradedate, expiration, strike, type, bid,ask,volume, open_interest,close from cboe_option where symbol=\''+sym+'\' and tradedate>=\''+startdate+'\' and tradedate<=\''+enddate+'\' and expiration>=curdate() and expiration <=curdate()+30 order by type,strike,expiration,tradedate')
        data={}
        for x in d:
            k= (x['type'],x['expiration'],x['strike'])
            if k not in data.keys():
                data[k]=[[],[],[],[],[],[],[]]
            data[k][0].append(x['tradedate'])
            data[k][1].append(x['volume'])
            data[k][2].append((x['bid']+x['ask'])/2)
            data[k][3].append(x['strike'])
            data[k][4].append(x['close'])
            data[k][5].append(x['expiration'])
            data[k][6].append(x['type'])
        
        largev=[]
        for x in data:
            _x=[_xx for _xx in data[x][1] if _xx >100]
            if len(_x)>1:
                __x=np.max(np.array(_x[1:])/np.array(_x[:-1]))
                if __x>5:
                    largev.append([x,_x])
        largev=sorted(largev,key=lambda _x: _x[1], reverse=True)
        
        
        plt.rcParams['figure.figsize'] = areadim
        fig=plt.figure()
        
        item=largev[0]
        n=0
        allstrike=np.array(list(set([x['strike'] for x in d])))
        print(cnt)
        print(len(largev))
        for item in largev[:cnt*3]:  
            if n==cnt+1:
                break
            item=data[largev[n][0]]
            item=np.array(item)
            days=[x.days for x in item[5]-item[0]]
            sig=[self.et.solve_option_sig(item[3][x],item[4][x],0.05,max(1,(item[5][x]-item[0][x]).days)/365,item[2][x],call=item[6][x]=='call',xtol=1e-10)  for x in range(len(item[0]))]
            strike2=[]
            sigratio=[]
            for nn in range(len(item[0])):
#               print('>')
#               print(np.array([np.abs(item[4][n]*2-item[3][n]-_x) for _x in allstrike]))
                ind=min(range(len(allstrike)),key=lambda _x: np.abs(item[4][nn]*2-item[3][nn]-allstrike[_x]))
#               print(ind)
#               print(allstrike[ind])
#               print(str(item[4])+" "+str(item[3]))
#               print('<')
                strike2=allstrike[ind]

#           print(item[4])
#           strike2=np.argmin(allstrike,lambda _x: np.abs(item[4]*2-item[3]-_x))
#           strike2=allstrike[strike2]
                k=('put' if item[6][n]=='call' else 'call',item[5][n],strike2)
                if not k in data.keys():
                    continue
                item2=np.array(data[k])
                sig2=self.et.solve_option_sig(item2[3][n],item2[4][n],0.05,max(1,(item2[5][n]-item2[0][n]).days)/365,item2[2][n],call=item2[6][n]=='call',xtol=1e-10) 
                if sig[n]==0 or sig2==0: 
                    continue
                _sigratio=sig[n]/sig2 if item[6][n]=='put' else sig2/sig[n]
                sigratio.append(_sigratio)
            if len(sigratio) != len(item[0]):
                continue


            ax=fig.add_subplot(cnt,1,n+1)
            ax2=ax.twinx()
            ax3=ax.twinx()
            ax.set_xlabel('Trade Date')
            ax.set_ylabel('volumn')
            ax.bar(item[0],item[1],label='volume',color='green' if item[6][0]=='call' else 'orange')
        
            maxind=np.argmax(item[1])
            ax.bar(item[0][maxind:maxind+1],item[1][maxind:maxind+1],color='red')
        
            ax2.plot(item[0],sigratio,'-o',color='blue')
            ax2.set_ylabel("Put-to-Call\nVolatility Skew ")
#           ax2.set_ylim(0)
            ax2.yaxis.set_major_formatter(mtick.ScalarFormatter(1))
            ax3.plot(item[0],item[4],'-o',color='grey')
            ax3.set_ylabel("Stock Price")
            ax3.spines['top'].set_visible(False)
            ax3.spines['bottom'].set_visible(False)
            ax3.spines['left'].set_visible(False)
#           ax3.spines['right'].set_position(('outward', 60))
            ax3.spines['right'].set_position('center')
            ax3.spines['right'].set_color('grey')
            plt.title(sym+" "+str(item[6][0])+"  Strike "+str(item[3][0])+" Expiration "+str(item[5][0]))
            n=n+1
        fig.tight_layout()



    def plt_cal_iv(self,sym,tradedate,expiration,span=1.2):
        c=self.et.db_fetch('select (ask+ask)/2 as price, close,strike,volume,expiration-tradedate+1 from cboe_option where symbol=\''+sym+'\' and tradedate=\''+tradedate+'\' and expiration=\''+expiration+'\' and type=\'call\' and bid<>0 and ask<>0 and volume<>0 order by strike')
        c=[x for x in c if np.abs(x[2]-x[1])/x[1]<(span-1.0)]
        sig_c=np.array([self.et.solve_option_sig(x[2],x[1],0.05,x[4]/365,x[0],call=True,ini=.5) for x in c ])
        c=np.array([list(x) for x in zip(*c)])
        
        p=self.et.db_fetch('select (ask+ask)/2 as price, close,strike,volume, expiration-tradedate+1 from cboe_option where symbol=\''+sym+'\' and tradedate=\''+tradedate+'\' and expiration=\''+expiration+'\' and type=\'put\'  and bid<>0 and ask<>0 and volume <>0 order by strike')
        p=[x for x in p if np.abs(x[2]-x[1])/x[1]<(span-1.0)]
        sig_p=np.array([self.et.solve_option_sig(x[2],x[1],0.05,x[4]/365,x[0],call=False,ini=.5) for x in p])
        p=np.array([list(x) for x in zip(*p)])
        close=c[0][1]
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax.plot(c[2]-c[1],sig_c,'-o',label='call',color='blue')
        ax.plot(p[1]-p[2],sig_p,'-o',label='put',color='orange')
        ax.plot(-(c[2]-c[1]),sig_c,':',label='call',color='blue',alpha=0.3)
        ax.plot(-(p[1]-p[2]),sig_p,':',label='put',color='orange',alpha=0.3)
        ax2.bar(c[2]-c[1],c[3],label='call',color='blue',alpha=0.5)
        ax2.bar(p[1]-p[2],p[3],label='put',color='orange',alpha=0.5)
        ax.plot([0,0],ax.get_ylim(),'--',color='grey',alpha=0.4,linewidth=1)
        plt.text(0.3,0.8, "In the Money",transform=fig.transFigure)
        plt.text(0.7,0.8, "out of the Money",transform=fig.transFigure)
        plt.title(sym+" expiration: "+str(expiration)+" tradedate: "+str(tradedate))
        plt.legend(loc='best')
        plt.show()


    def plt_cal_iv_skew(self,sym,tradedate,expiration,span=1.2):
        c=self.et.db_fetch('select (ask+ask)/2 as price, close,strike,volume,greatest(expiration-tradedate,1) from cboe_option where symbol=\''+sym+'\' and tradedate=\''+tradedate+'\' and expiration=\''+expiration+'\' and type=\'call\' and bid<>0 and ask<>0 and volume>100 order by strike')
        c=[x for x in c if np.abs(x[2]-x[1])/x[1]<(span-1.0)]
        median=np.median([x[3] for x in c])
        c=[x for x in c if x[3]>median*0.1]
        sig_c = np.array([c.impliedVolatility for c in [mibian.BS([x[1], x[2], 5, x[4]], callPrice= x[0]) for x in c ]])/100
        #sig_c=np.array([et.solve_option_sig(x[2],x[1],0.05,x[4]/365,x[0],call=True,ini=.5,xtol=1e-5) for x in c ])
        
        c=np.array([list(x) for x in zip(*c)])
        
        p=self.et.db_fetch('select (ask+ask)/2 as price, close,strike,volume, greatest(1,expiration-tradedate) from cboe_option where symbol=\''+sym+'\' and tradedate=\''+tradedate+'\' and expiration=\''+expiration+'\' and type=\'put\'  and bid<>0 and ask<>0 and volume>100 order by strike')
        p=[x for x in p if np.abs(x[2]-x[1])/x[1]<(span-1.0)]
        median=np.median([x[3] for x in p])
        
        
        p=[x for x in p if x[3]>median*0.1]
        
        sig_p = np.array([p.impliedVolatility for p in [mibian.BS([x[1], x[2], 5, x[4]], putPrice= x[0]) for x in p ]])/100
        #sig_p=np.array([et.solve_option_sig(x[2],x[1],0.05,x[4]/365,x[0],call=False,ini=.5,xtol=1e-5) for x in p])
        p=np.array([list(x) for x in zip(*p)])
        
        close=c[1][0]
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax3=ax.twinx()
        ax.plot(c[2]-c[1],sig_c,'-o',label='call Implied Volatility', color='blue',alpha=0.5)
        ax.plot(p[1]-p[2],sig_p,'-o',label='put Implied Volatility', color='orange',alpha=0.5)
        ax.plot(-(c[2]-c[1]),sig_c,':',label='Mirrored call Implied Volatility',color='blue',alpha=0.3)
        ax.plot(-(p[1]-p[2]),sig_p,':',label='Mirrored Put Implied Volatility',color='orange',alpha=0.3)
        ax.set_ylabel("Implied Volatility")
        w=np.min(c[2][1:]-c[2][:-1])/2
        
        ax2.bar(c[2]-c[1],c[3],width=w,label='Call Volume',color='blue',alpha=0.5)
        ax2.bar(p[1]-p[2],p[3],width=w,label='Put Volume',color='orange',alpha=0.5)
        #ax.plot([0,0],ax.get_ylim(),'--',color='grey',alpha=0.4,linewidth=1)
        ax2.set_ylabel("Volume")
        #ax3.plot(c[2]-c[1],c[0],color='blue')
        #ax3.plot(p[1]-p[2],p[0],color='orange')
        
        ax3.plot(c[2]-c[1],c[0]+c[2]-c[1],'--',label='Call Excercise Cost',color='blue')
        ax3.plot(p[1]-p[2],-(-p[0]+p[2]-p[1]),'--',label='Put Excercise Cost',color='orange')
        ax3.set_ylabel("Excercise Cost")
        plt.text(0.45,0.8, "ITM",fontsize=15,transform=fig.transFigure,zorder=100)
        plt.text(0.55,0.8, "OTM",fontsize=15,transform=fig.transFigure,zorder=100)
        plt.title(sym.upper()+"\nStock: "+str(close)+" Expiration: "+str(expiration)+" Tradedate: "+str(tradedate))
        line,label=ax.get_legend_handles_labels()
        line2,label2=ax2.get_legend_handles_labels()
        line3,label3=ax3.get_legend_handles_labels()
        ax3.get_yaxis().set_visible(False)
        ax3.get_yaxis().set_ticks([])
#       ax2.axis('off')
#       ax3.spines['top'].set_visible(False)
#       ax3.spines['bottom'].set_visible(False)
#       ax3.spines['left'].set_visible(False)
#       #ax3.spines['right'].set_position(('outward', 60))
#       ax3.spines['right'].set_position('center')
#       ax3.spines['right'].set_color('grey')
        
        ax.patch.set_alpha(0.0)
        ax2.patch.set_alpha(0.0)
        ax3.patch.set_alpha(0.0)

        xx=[]
        xx.append(c[2]-c[1])
        xx.append(sig_c)
        xx.append(p[1]-p[2])
        xx.append(sig_p)
        print(xx)
        print(min(xx[0]))
        model_10_c = self.et.get_natural_cubic_spline_model(xx[0], xx[1], minval=float(min(xx[0])), maxval=float(max(xx[0])), n_knots=10)
        model_10_p = self.et.get_natural_cubic_spline_model(xx[2], xx[3], minval=float(min(xx[2])), maxval=float(max(xx[2])), n_knots=10)
        #X_Y_Spline=make_interp_spline(xx[0],xx[1])
        #X_=np.linspace(xx[0].min(),xx[0].max(),500)
        #Y_=X_Y_Spline(X_)
        #Y_=model_10(xx[0])
        #ax.plot(X_,Y_,color='purple')
        ax.plot(xx[0],model_10_c.predict(xx[0]),color='blue')
        ax.plot(xx[2],model_10_p.predict(xx[2]),color='orange')

        plt.legend(line+line2+line3,label+label2+label3,loc='best')
#       plt.show()
        return fig

    def plt_max_pain_history(self,sym,expiration):
        tradedate=np.array([x[0] for x in self.et.db_fetch('select distinct tradedate from cboe_option where symbol="'+sym+'" and expiration="'+expiration+'"')])
        
        price=[list(self.et.maxpain(sym,expiration,t.strftime('%Y-%m-%d')))[0] for t in tradedate]
        fig,ax=plt.subplots()
        ax.plot(tradedate,price,'-o',color="red", label="Max Pain",zorder=2)
        ax2=ax.twinx()
        
        stock=self.et.db_fetch("select tradedate,close from cboe_stock where symbol='"+sym+"' and tradedate in ('"+"','".join([t.strftime('%Y-%m-%d') for t in tradedate])+"') order by tradedate")
        stock=[list(x) for x in zip(*stock)]
        ax.plot(stock[0],stock[1],'-o',color='blue',label="Stock Price",zorder=2)
        ax.grid()
#       ax.tick_params(labelright=True)
        oi=self.et.db_fetch("select tradedate,sum(open_interest) from cboe_option where symbol='"+sym+"' and tradedate in ('"+"','".join([t.strftime('%Y-%m-%d') for t in tradedate])+"') group by tradedate order by tradedate")
        print(oi)
        oi=[list(x) for x in zip(*oi)]
        ax2.plot(oi[0],oi[1],'-o',color='grey',label="Open Interest",zorder=1)
        ln1,lb1=ax.get_legend_handles_labels()
        ln2,lb2=ax2.get_legend_handles_labels()


        ax.legend(ln1+ln2,lb1+lb2,loc='upper left')
        plt.title(sym.upper()+'\nStock:  '+str(stock[1][-1])+' Max Pain price: '+str(price[-1])+" Expiration: "+expiration)
        return fig,plt

    def plt_max_pain(self,sym,expiration,tradedate):
        price,ind,call,put=self.et.maxpain(sym,expiration,tradedate)
        width=np.min(call[0][1:]-call[0][:-1])/2
        fig,ax=plt.subplots()
        ax.bar(put[0],call[1]+put[1],color='red',label='Put',zorder=0,width=width)
        ax.bar(call[0],call[1],color='green',label='Call',zorder=1,width=width)
        
        ax.tick_params(labelright=True)
        ax.grid()
        ax.legend(loc='upper left')
        plt.title(sym.upper()+"\nMax Pain price: \$"+str(price)+" Expiration: "+expiration+ "Tradedate: "+str(tradedate))
        return fig

    def plt_momentum_signal_trade(self,sym,span=200,db='stock',indicator=True):
        r=self.et.momentum_ma_signal(sym,span=span,db='stock')
        ret=r['data']['indicator']
        trade=r['trade']
        sum([x['exitprice']-x['entryprice'] for x in r['trade']])/r['trade'][0]['entryprice']
        fig,ax=plt.subplots()
        ax2=ax.twinx()
        ax2.set_facecolor('none')
        if indicator:
            ax.plot(r['data']['date'][-span:],ret[0][-span:],label='Weekly',zorder=2)
            ax.plot(r['data']['date'][-span:],ret[1][-span:],label='Monthly',zorder=2)
            ax.plot(r['data']['date'][-span:],ret[2][-span:],label='Quarterly',zorder=2)
        ax2.plot(r['data']['date'][-span:],r['data']['close'][-span:],color='grey',label='stock',zorder=1)
        
        labelon=True
        for x in trade:
            if labelon:
                labelon=False
                ax2.plot(r['data']['date'][x['entryindex']:x['exitindex']+1],r['data']['close'][x['entryindex']:x['exitindex']+1],linewidth=4,color='red',label='Trade')
            else:
                ax2.plot(r['data']['date'][x['entryindex']:x['exitindex']+1],r['data']['close'][x['entryindex']:x['exitindex']+1],linewidth=4,color='red')
        gain=np.prod([x['gain']+1 for x in r['trade']])-1
        holdgain=r['data']['close'][-1]/r['data']['close'][r['trade'][0]['entryindex']]-1
        ax.legend()
        ax2.legend()
#       ax.set_title(sym+" Gain:  %5.1f%% signal trade vs %5.1f%% buy&hold" % (gain*100,holdgain*100),y=0.7)
        plt.title(sym+" Gain:  %5.1f%% signal trade vs %5.1f%% buy&hold" % (gain*100,holdgain*100))
        return fig

