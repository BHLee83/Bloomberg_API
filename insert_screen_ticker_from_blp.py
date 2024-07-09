import pymysql
import json

from datetime import datetime

import blpapi
from optparse import OptionParser

today = datetime.today()
proc_date = today.strftime('%Y-%m-%d')
proc_time = today.strftime('%H%M%S%f')


def parseCmdLine():
    parser = OptionParser(description="Retrieve reference data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)

    (options, args) = parser.parse_args()

    return options

def db_connect():
    
    conn = pymysql.connect(
            host='172.22.30.56', 
            port=3306, 
            user='root', 
            passwd='9999',
            db='',
            charset='utf8')
    
    return conn;
    

def db_close(conn):
    
    if conn:
        conn.close()

    
def getTickerList(conn):

    try:    
        curr = conn.cursor()
        
        curr.execute("SELECT TickerName, Description FROM ficc.bloomberg_tickers WHERE IsUsed = 'Y'")
        
        ret = curr.fetchall()

        return ret

    except Exception as e:
        raise e
        
    finally:

        if curr:
            curr.close()
    

def insertTickerRT(conn, ticker_list, bdp_list):
    
    sql_del = """
        DELETE FROM ficc.raw_bloomberg WHERE DateStr = %(date_str)s
        """
        
    sql_ins = """
        INSERT INTO ficc.raw_bloomberg (
            DateStr,
            Ticker,
            Tag,
            Description,
            Value
        )
        VALUES
        (
            %(date_str)s,
            %(ticker)s,
            %(tag)s,
            %(description)s,
            %(value)s
        )
        """
        
        
    try:    

        curr = conn.cursor()

        params = {
            'date_str': proc_date
        }
        
        curr.execute(sql_del, params)

        for bdp in bdp_list:

	
            if bdp['value'] is not None and type(bdp['value']) in (float, int):

                print(bdp)
	

                params = {
                    'date_str': proc_date,
                    'ticker': bdp['security'],
                    'tag': bdp['tag'],
                    'description': bdp['description'],
                    'value': bdp['value']
                }
                curr.execute(sql_ins, params)
                
        conn.commit()
        return 0

    except Exception as e:
        raise e
        
    finally:
        
        if curr:
            curr.close()


   

def main():

    global options
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print ("Connecting to %s:%d" % (options.host, options.port))

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print ("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print ("Failed to open //blp/refdata")
        return

    refDataService = session.getService("//blp/refdata")
    request = refDataService.createRequest("ReferenceDataRequest")
    
    conn = db_connect()
    
    ticker_list = getTickerList(conn)
    
    # append securities to request
    for ticker_info in ticker_list:
        request.append("securities", ticker_info[0])
        




    # append fields to request
    request.append("fields", "PX_LAST")
    request.append("fields", "DS002")

    print ("Sending Request:", request)
    session.sendRequest(request)

    try:
        
        bdp_list = []
        
        # Process received events
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement('securityData'):
                    securities = msg.getElement('securityData')
                    for security in securities.values():
                        field = security.getElement('fieldData')

                        description = ''
                        px_last = (None,)

                        if field.hasElement('DS002'):
                            description = field.getElementAsString('DS002')

                        if field.hasElement('PX_LAST'):
                            px_last = field.getElementAsFloat('PX_LAST'),
                   
                        index = {
                            'security': security.getElementAsString('security'),
                            'value': px_last[0],
                            'description': description,
                            'tag': 'PX_LAST'
                            }
                        
                        #print (index)
                        
                        bdp_list.append(index)
                    
                       
            # Response completly received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
            
        
        insertTickerRT(conn, ticker_list, bdp_list)



    finally:
        # Stop the session
        session.stop()
        db_close(conn)

if __name__ == "__main__":

    print ('###########################################################')
    print ('Gethering bloomberg real-time ticker : {0}-{1}'.format(
        proc_date, proc_time))

    try:
        
        main()

    except KeyboardInterrupt:
        print ("Ctrl+C pressed. Stopping...")
        exit(1)
        
    except Exception as e:
        print (e)
        exit(2)

    finally:
        print ('Ok.. good job, bye')
        print ('###########################################################')
        # to-do : notify by slack


__copyright__ = """
NO RITGHT.
"""
