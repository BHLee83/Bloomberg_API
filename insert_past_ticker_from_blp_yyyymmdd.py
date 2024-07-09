# -*- coding: utf-8 -*-

import pymysql
import blpapi

from optparse import OptionParser

from datetime import datetime, timedelta
import socket


today = datetime(2020, 12, 30)
yesterday = today + timedelta(days=-1)
bef_7days = today + timedelta(days=-1)

proc_date = today.strftime('%Y-%m-%d')
proc_time = today.strftime('%H%M%S%f')


def getIpAddr():

    s =  socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ipinfo = s.getsockname()
    s.close()

    return ipinfo[0]


def parseCmdLine():

    parser = OptionParser(description='Retrieve historical data.')
    parser.add_option('-a',
                      '--ip',
                      dest='host',
                      help='server name or IP (default: %default))',
                      metavar='ipAddress',
                      default='localhost')
    parser.add_option('-p',
                      dest='port',
                      type='int',
                      help='server port (default: %default))',
                      metavar='tcpPort',
                      default=8194)

    (options, args) = parser.parse_args()

    return options



def db_connect():

    conn = pymysql.connect(
            host='172.22.30.56',
            port=3306,
            user='root',
            passwd='9999',
            charset='utf8')

    return conn


def db_close(conn):
    if conn:
        conn.close()


def getTickerList(conn):

    try:
        curr = conn.cursor()

        sqlstr = ''
        sqlstr = "select TickerName, Description from ficc.bloomberg_tickers where IsUsed='Y'"

        curr.execute(sqlstr)

        ret = curr.fetchall()
        return ret

    except Exception as e:
        print ('In getTickerList, {}'.format(e))
        raise e

    finally:

        if curr:
            curr.close()


def insertTickerRT(conn, bdp_list):

    sql_exists = ("""

        select
            *
        from
            ficc.raw_bloomberg as a
        where 1=1
            and a.DateStr = %(datestr)s
            and a.Ticker = %(ticker)s
            and a.Tag = %(tag)s

    """)

    sql_ins = ("""

        insert into ficc.raw_bloomberg (
            DateStr,
            Ticker,
            Tag,
            Description,
            Value
        ) values (
            %(datestr)s,
            %(ticker)s,
            %(tag)s,
            %(description)s,
            %(value)s
        )
    """)


    try:

        curr = conn.cursor()


        for bdp in bdp_list:
            if bdp['value'] is not None:

                params = {
                    'datestr': bdp['datestr'],
                    'ticker': bdp['security'],
                    'tag': bdp['tag'],
                }
                numrows = curr.execute(sql_exists, params)

                if numrows > 0:
                    print('It is already inserted. {}'.format(bdp))
                    continue

                params = {
                    'datestr': bdp['datestr'],
                    'ticker': bdp['security'],
                    'tag': bdp['tag'],
                    'description': bdp['description'],
                    'value': bdp['value'],
                }
                curr.execute(sql_ins, params)

        conn.commit()

    except Exception as e:
        print('In insertTickerRT, {}'.format(e))
        raise e

    finally:
        if curr:
            curr.close()


def main():

    options = parseCmdLine()

    print (options)

    # Fill sessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print ('Connecting to %s:%d' % (options.host, options.port))

    try:

        # Create a session
        session = blpapi.Session(sessionOptions)

        # Start a session
        if not session.start():
            print ('Failed to start session............')
            return

        if not session.openService('//blp/refdata'):
            print ('Failed to open //blp/refdata..................')
            return


        refDataService = session.getService('//blp/refdata')
        request = refDataService.createRequest('HistoricalDataRequest')


        conn = db_connect()

        ticker_list = getTickerList(conn)
        tag_list = ['PX_LAST','PX_OPEN','PX_HIGH','PX_LOW']
        ticker_desc = {}

        #print (ticker_list)

        # append securities to request
        for ticker_info in ticker_list:
            request.append('securities', ticker_info[0])
            ticker_desc.update({ticker_info[0]: ticker_info[1]})

        request.set('startDate', bef_7days.strftime('%Y%m%d'))
        #request.set('startDate', yesterday.strftime('%Y%m%d'))
        request.set('endDate', yesterday.strftime('%Y%m%d'))

        # append fields to request
        for tag in tag_list:
            request.append('fields', tag)

        print ('Sending Request: ', request)
        session.sendRequest(request)

        ipaddr = getIpAddr().split('.')

        bdp_list = []

        while(True):

            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                # print (msg)
                if msg.hasElement('securityData'):
                    securities = msg.getElement('securityData')

                    if securities.hasElement('security'):
                        ticker_name = securities.getElementAsString('security')

                    # print (ticker_name)

                    if securities.hasElement('fieldData'):
                        fields = securities.getElement('fieldData')

                        for field in fields.values():

                            if field.hasElement('date'):
                                datestr = field.getElementAsString('date')

                            for tag in tag_list:
                                if field.hasElement(tag):
                                    px_value = field.getElementAsFloat(tag)

                                    index = {
                                        'security': ticker_name,
                                        'datestr': datestr,
                                        'tag': tag,
                                        'value': px_value,
                                        'description': ticker_desc[ticker_name],
                                    }

                                    bdp_list.append(index)

            # Response completly received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break


        print (bdp_list)
        insertTickerRT(conn, bdp_list)


    except Exception as e:
        print ('In main, {}'.format(e))
        raise e

    finally:
        # Stop the session
        session.stop()
        db_close(conn)






if __name__ == '__main__':

    try:
        print('#######################################################')
        print('Gethering bloomberg past ticker : {0}-{1}'.format(
            proc_date, proc_time
        ))

        main()

    except KeyboardInterrupt:
        print ('Ctrl+C pressed. Stopping.....')
        exit(1)

    except Exception as e:
        print (e)
        exit(2)

    finally:
        print('OK.. good job. Bye')
        print('#######################################################')
