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
    enddate=None
    span=-7
    if 'sym' in o.keys():
        sym=o['sym'][0]
    if 'enddate' in o.keys():
        enddate=o['enddate'][0]
    if 'span' in o.keys():
        span=int(o['span'][0])
    
    try:
        fig=ui.plt_stock_intraday(sym,enddate=enddate,span=span)
        html_str=mpld3.fig_to_html(fig)
        html = "<html>"
        html += html_str
        html += '</html>'
        html = bytes(html, encoding= 'utf-8')
    except Exception as ex:
        html ='<html>'+str(ex)+' '+enddate+' '+span+'</html>'
        html = bytes(html, encoding= 'utf-8')

    response_headers = [('Content-type', 'text/html'),
                        ('Content-Length', str(len(html)))]
    start_response(status, response_headers)

    return [html]
