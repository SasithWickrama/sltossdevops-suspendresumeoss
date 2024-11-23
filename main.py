import multiprocessing
import random
import sys
from datetime import datetime
from resume import Resume
from suspend import Suspend
import db

conn = db.DbConnection.dbconnPrg("")
cmonth = datetime.now().strftime('%Y%m')
data = {}


def specific_string(length):
    sample_string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'  # define the specific string
    # define the condition for random string
    return ''.join((random.choice(sample_string)) for x in range(length))


def suspend(typ, x):
    if typ == 'VOICE':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_VOICE_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')' \
               'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_VOICE_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'BB':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_BB_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_BB_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'IPTV':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_IPTV_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL SUSPEND\',\'SUSPEND\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_IPTV_' + cmonth + '.ROWID), 5) = ' + str(x)

    c = conn.cursor()
    c.execute(sql)

    for row in c:
        ROWID,LEA,SERVICE_ID, REQ_BY, SERVICE_TYPE, ORDER_TYPE, CR, ACCNO, CCT, STATUS = row

        data['ROWID'] = ROWID
        data['LEA'] = LEA
        data['SERVICE_ID'] = SERVICE_ID
        data['REQ_BY'] = REQ_BY
        data['SERVICE_TYPE'] = SERVICE_TYPE
        data['ORDER_TYPE'] = ORDER_TYPE
        data['CR'] = CR
        data['ACCNO'] = ACCNO
        data['TPNO'] = CCT
        data['STATUS'] = STATUS

        refid = specific_string(15)
        data['LOGREF'] = refid

        print(data)

        if STATUS == 150:
            result = Suspend.ossUpdate(data)
            print(result)
            if result == 0:
                if typ == 'VOICE':
                    sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [101, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'BB':
                    sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'IPTV':
                    sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
            else:
                if typ == 'VOICE':
                    sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'BB':
                    sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'IPTV':
                    sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)

        if STATUS == 200:
            result = Suspend.seviceOrderCreate(data)

            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)


def resume(typ, x):
    if typ == 'VOICE':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_VOICE_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
               'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_VOICE_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'BB':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_BB_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_BB_' + cmonth + '.ROWID), 5) = ' + str(x)
    if typ == 'IPTV':
        sql = 'select ROWID,LEA,SERVICE_ID,REQ_BY,SERVICE_TYPE,ORDER_TYPE,CR,ACCNO ,CCT,STATUS ' \
              'from  EXPROV_IPTV_' + cmonth + ' where STATUS IN(150,200) AND ORDER_TYPE IN (\'MODI-PARTIAL RESUME\',\'RESUME\')' \
              'AND MOD(DBMS_ROWID.ROWID_ROW_NUMBER(EXPROV_IPTV_' + cmonth + '.ROWID), 5) = ' + str(x)
    c = conn.cursor()
    c.execute(sql)

    for row in c:
        ROWID,LEA,SERVICE_ID, REQ_BY, SERVICE_TYPE, ORDER_TYPE, CR, ACCNO, CCT, STATUS = row

        data['ROWID'] = ROWID
        data['LEA'] = LEA
        data['SERVICE_ID'] = SERVICE_ID
        data['REQ_BY'] = REQ_BY
        data['SERVICE_TYPE'] = SERVICE_TYPE
        data['ORDER_TYPE'] = ORDER_TYPE
        data['CR'] = CR
        data['ACCNO'] = ACCNO
        data['TPNO'] = CCT
        data['STATUS'] = STATUS

        refid = specific_string(15)
        data['LOGREF'] = refid

        print(data)

        if STATUS == 150:
            result = Resume.ossUpdate(data)
            print(result)
            if result == 0:
                if typ == 'VOICE':
                    sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [101, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'BB':
                    sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [101, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'IPTV':
                    sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS, STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [101, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
            else:
                if typ == 'VOICE':
                    sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'BB':
                    sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
                if typ == 'IPTV':
                    sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                    with conn.cursor() as cursor2:
                        cursor2.execute(sqlupdate, [111, ROWID, STATUS])
                        conn.commit()
                        print(cursor2.rowcount)
        if STATUS == 200:
            result = Resume.seviceOrderCreate(data)

            if typ == 'VOICE':
                sqlupdate = 'update EXPROV_VOICE_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'BB':
                sqlupdate = 'update EXPROV_BB_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)
            if typ == 'IPTV':
                sqlupdate = 'update EXPROV_IPTV_' + cmonth + ' set STATUS=:STATUS,EX_ID=:EX_ID,STATUS_DATE=sysdate where  ROWID= :ROW_ID and STATUS=:STAT'
                with conn.cursor() as cursor2:
                    cursor2.execute(sqlupdate, [201,result, ROWID, STATUS])
                    conn.commit()
                    print(cursor2.rowcount)


if __name__ == '__main__':
    processes = []

    if sys.argv[1] == 'SUSPEND':
        for i in range(0, 5):
            if sys.argv[2] == 'VOICE':
                p = multiprocessing.Process(target=suspend, args=('VOICE', i,))

            if sys.argv[2] == 'BB':
                p = multiprocessing.Process(target=suspend, args=('BB', i,))

            if sys.argv[2] == 'IPTV':
                p = multiprocessing.Process(target=suspend, args=('IPTV', i,))

            processes.append(p)
            p.start()

    if sys.argv[1] == 'RESUME':
        for i in range(0, 5):
            if sys.argv[2] == 'VOICE':
                p = multiprocessing.Process(target=resume, args=('VOICE', i,))

            if sys.argv[2] == 'BB':
                p = multiprocessing.Process(target=resume, args=('BB', i,))

            if sys.argv[2] == 'IPTV':
                p = multiprocessing.Process(target=resume, args=('IPTV', i,))

            processes.append(p)
            p.start()

    # multiprocessing_func(i)

        for process in processes:
            process.join()
