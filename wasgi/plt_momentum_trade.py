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
    if 'span' in o.keys():
        span=int(o['span'][0])
    else:
        span=500
    indicator=False
    if 'indicator' in o.keys():
        if o['indicator'][0]=='on':
            indicator=True
#   
    try:
#       fig=ui.plt_momentum_signal_trade(sym,span,indicator!=0)
        fig=ui.plt_momentum_signal_trade(sym,span=span,indicator=indicator)
        html_str=mpld3.fig_to_html(fig)
        html = "<html>"
        html += html_str
        html += '</html>'
        html = bytes(html, encoding= 'utf-8')
    except Exception as e:
        html =bytes('<html>No Result:'+str(e)+'</html>',encoding='utf-8')

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(html)))]
    start_response(status, response_headers)

    return [html]
