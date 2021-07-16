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
    expiration='2021-07-16'
    if 'sym' in o.keys():
        sym=o['sym'][0]
    if 'expiration' in o.keys():
        expiration=o['expiration'][0]
    
    try:
        fig=ui.plt_max_pain_history(sym,expiration)
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
