import cx_Oracle
import const

class DbConnection:

    def dbconnPrg(self):
        try:
            hostname = const.dbhost
            port = const.dbport
            service = const.dbservice
            user = const.dbuser
            password = const.dbpwd

            dsn_tns = cx_Oracle.makedsn(hostname, port, service_name=service)
            conn = cx_Oracle.connect(user=user, password=password, dsn=dsn_tns)
            return conn
        except Exception as e:
            print("Exception : %s" % e)
            return e

