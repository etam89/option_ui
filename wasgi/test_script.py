#import numpy 

from cgi import parse_qs
import sys
sys.path.append('/var/www/stock')
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

import etoption
et=etoption.etoption(database='finance',user='root',passwd='Password.11');

def application(environ, start_response):
    status = '200 OK'
    output = b'Hello World!'

    o=parse_qs(environ['QUERY_STRING'])
    sym='X'
    startdate='2021-07-12'
    expiration='2021-07-16'
    if 'sym' in o.keys():
        sym=o['sym'][0]
    if 'startdate' in o.keys():
        startdate=o['startdate'][0]
    if 'expiration' in o.keys():
        expiration=o['expiration'][0]
    
    et.db_init(database='finance',user='root',passwd='Password.11')
    sym='X'
    expiration,tradedate,S,IV,df=et.option_strangle(sym,tradedate=startdate,expiration=expiration,usedb='cboe_option')
    df.drop(df.columns[40:],axis=1).drop(df.index[60:],axis=0)
    html = "<html>This "
    html += "is the code"
    html += "\n"+"You are using Python {}.{}.".format(sys.version_info.major, sys.version_info.minor)
    html += df.to_html()
    html += '</html>'
    html = bytes(html, encoding= 'utf-8')

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(html)))]
    start_response(status, response_headers)

    return [html]
