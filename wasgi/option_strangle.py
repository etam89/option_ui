#import numpy 

from cgi import parse_qs
import sys
sys.path.append('/var/www/stock')
import datetime
import numpy as np
import os

import etoption
import mibian
import uioption
import mpld3

ui=uioption.uioption(database='finance',user='root',passwd='Password.11')

def application(environ, start_response):
    status = '200 OK'
    output = b'Hello World!'

    o=parse_qs(environ['QUERY_STRING'])
    sym='X'
#   indicator=1
    if 'sym' in o.keys():
        sym=o['sym'][0]
#   
    try:
#       fig=ui.plt_momentum_signal_trade(sym,span,indicator!=0)
        _,_,S,iv,df=ui.et.option_strangle_online(sym)
        df=df.drop(df.columns[18:],axis=1).drop(df.index[25:],axis=0)
        html_str=df.to_html()
        html = "<html>"
        html +='Price: %5.2f, IV: %5.2f%% ($%5.2f)' % (S,iv*100,S*iv)
        html += html_str
        html += '</html>'
        html = bytes(html, encoding= 'utf-8')
    except Exception as e:
        html =bytes('<html>No Result:'+str(e)+'</html>',encoding='utf-8')

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(html)))]
    start_response(status, response_headers)

    return [html]
