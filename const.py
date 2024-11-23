from log import getLogger

dbhost='172.25.1.172'
dbport=1521
dbservice= 'clty'
dbuser='OSSPRG'
dbpwd='prgoss456'

voiceend = 'http://10.68.128.3:8080/spg'
bbend=''
iptvend=''

loggersus = getLogger('susoss', 'logs/susoss')
loggerres = getLogger('resoss', 'logs/resoss')