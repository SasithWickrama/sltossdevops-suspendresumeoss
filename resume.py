import random
import re
import subprocess

import zeep

import const
import requests
import db

conn = db.DbConnection.dbconnPrg("")


def specific_string(length):
    sample_string = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'  # define the specific string
    # define the condition for random string
    return ''.join((random.choice(sample_string)) for x in range(length))


class Resume:
    def ossUpdate(self):

        try:
            const.loggerres.info(self['LOGREF'] + "  " + "Start Suspend: =========================================================================")
            const.loggerres.info(self['LOGREF'] + "  " + str(self))

            if self['ORDER_TYPE'] == 'MODI-PARTIAL SUSPEND':
                const.loggerres.info(self[
                                      'LOGREF'] + "  " + "End   : =========================================================================")

                return 0

            if self['ORDER_TYPE'] == 'SUSPEND':
                sqlservice = 'update clarity.services set SERV_STAS_ABBREVIATION = :STAT,SERV_STATUS_DATE=sysdate ' \
                             'where SERV_ID=:SERV_ID and SERV_CUSR_ABBREVIATION = :CR and SERV_STAS_ABBREVIATION = :SERV_STAS_ABBREVIATION'
                with conn.cursor() as cursor:
                    cursor.execute(sqlservice, ['SUSPENDED', self['SERVICE_ID'], self['CR'], 'INSERVICE'])
                    conn.commit()
                    print(cursor.rowcount)

                sqlcircuit = 'update clarity.circuits set CIRT_STATUS = :STAT ' \
                             'where CIRT_SERV_ID=:SERV_ID and CIRT_CUSR_ABBREVIATION = :CR and CIRT_DISPLAYNAME = :TPNO and CIRT_STATUS = :PRESTAT'

                with conn.cursor() as cursor2:
                    cursor2.execute(sqlcircuit,
                                    ['SUSPENDED', self['SERVICE_ID'], self['CR'], self['TPNO'], 'INSERVICE'])
                    conn.commit()
                    print(cursor2.rowcount)

            const.loggerres.error(self['LOGREF'] + "  " + str(cursor.rowcount) + "  " + str(cursor2.rowcount))
            const.loggerres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")

            return 0


        except Exception as e:
            const.loggerres.error(self['LOGREF'] + "  " + str(e))
            const.loggerres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")
            return str(e)

    def seviceOrderCreate(self):

        exid = specific_string(15)
        SPEED = ''
        ARG = "ACC:" + self['ACCNO'] + "::LEA:" + self['LEA'] + "::CR:" + self['CR'] + "::EXID:" + exid + "::ORTYPE:" + \
              self['ORDER_TYPE'] + "::SPEED:" + SPEED + "::STYPE:" + self['SERVICE_TYPE'] + "::SVID:" + self[
                  'SERVICE_ID']
        try:
            result = subprocess.call(['java', '-jar', 'files/CreateSoClarity.jar', ARG])

            const.loggerres.error(self['LOGREF'] + "  " + str(result))
            const.loggerres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")

            return result
        except Exception as e:
            const.loggerres.error(self['LOGREF'] + "  " + str(e))
            const.loggerres.info(self[
                                  'LOGREF'] + "  " + "End   : =========================================================================")
            return str(e)
