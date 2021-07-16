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
    tradedate='2021-07-12'
    expiration='2021-07-16'
    span=1.2
    if 'sym' in o.keys():
        sym=o['sym'][0]
    if 'tradedate' in o.keys():
        tradedate=o['tradedate'][0]
    if 'expiration' in o.keys():
        expiration=o['expiration'][0]
    if 'span' in o.keys():
        span=o['span'][0]
    
    try:
        fig=ui.plt_cal_iv_skew(sym,tradedate,expiration,span=span)
        html_str=mpld3.fig_to_html(fig)
        html = "<html>"
        html += html_str
        html += '</html>'
        html = bytes(html, encoding= 'utf-8')
    except:
        html ='<html>No Result</html>'

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(html)))]
    start_response(status, response_headers)

    return [html]
