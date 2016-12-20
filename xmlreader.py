__author__ = 'michael'

from xml.dom import minidom

import sys, string
if len(sys.argv) != 3:
    print 'Usage: python xmlreader.py <password to db> <xml file>'
    sys.exit(1)

reload(sys)
sys.setdefaultencoding('utf-8')

passwd = str(sys.argv[1])
xml_file = str(sys.argv[2])

def processXml(xmldoc, detector, fields):
    insert_data =  '['
    for node in xmldoc.getElementsByTagName(detector):
        #print '\n'+sub_det_name+": "
        insert_data_str = '('
        for field in fields:
            statistic_to_exec   =   str(field)+'=node.attributes["'+field+'"]'
            exec    statistic_to_exec
            values_to_exec      =   'insert_data_str += "\'"+str('+field+'.value)+"\'"'
            exec    values_to_exec
            insert_data_str +=  ','
            print insert_data_str
        insert_data_str = insert_data_str[:-1]
        insert_data_str +=   '),'
        insert_data += insert_data_str
    insert_data +=  ']'
    return insert_data

def dbWrite2(dbConn, tableName, fields, data):
    dbCursor = dbConn.cursor()
    query = 'INSERT INTO ' + tableName.lower() + ' '
    brk1 = '('
    brk2 = '('
    for field in fields:
        brk1 += (field + ',')
        brk2 += '%s ,'
    brk1 = brk1[:-1]
    brk2 = brk2[:-1]

    query += brk1
    query += ') VALUES'
    query += brk2
    query += ')'
    print query
    dbCursor.executemany(query, eval(data))
    dbConn.commit()


def dbWrite(dbConn, tableName, data):
    dbCursor = dbConn.cursor()
    dbCursor.executemany('INSERT INTO ' + tableName.lower() + ' (fldCode,fldParent,fldName, fldDesc, fldTarget, fldIcon, fldLink, fldType, fldPosition)\
     VALUES(%s, %s ,%s, %s, %s, %s ,%s, %s, %s)', eval(data))


xmldoc = minidom.parse(xml_file)
tables = ['tblNode', 'tblDictionary', 'tblRole', 'tblUser', 'tblUserRoleLink', 'tblDept',
          'tblUserDeptLink', 'tblSysParameter', 'tblSysJob']
fields = ['fldCode','fldParent','fldName', 'fldDesc', 'fldTarget', 'fldIcon', 'fldLink', 'fldType', 'fldPosition']


import MySQLdb
conn = MySQLdb.connect(
    host="localhost",
    port=3306,
    user="root",
    passwd=passwd,
    db="callcenter"
)

for table in tables:
    #table = 'tblNode'
    print "processing ====> " + table
    if table == 'tblDictionary':
        fields = ['fldId', 'fldType', 'fldName', 'fldValue', 'fldStatus', 'fldComment']

    if table == 'tblRole':
        fields = ['fldId', 'fldRoleName']

    if table == 'tblUser':
        fields = ['fldId', 'fldLoginName', 'fldUserName', 'fldPassword', 'fldLoginStatus',
                  'fldUserStatus', 'fldLoginErrCount', 'fldModifyPwdCount', 'fldGender',
                  'fldEmail', 'fldPhone', 'fldMobile', 'fldFax', 'fldAddress', 'fldPosition']

    if table == 'tblUserRoleLink':
        fields = ['fldId', 'fldUserId', 'fldRoleId']

    if table == 'tblDept':
        fields = ['fldDeptCode', 'fldParent', 'fldDeptName', 'fldPosition']

    if table == 'tblUserDeptLink':
        fields = ['fldId', 'fldLoginName', 'fldDeptCode']

    if table == 'tblSysParameter':
        fields = ['fldId', 'fldName', 'fldValue', 'fldUnit', 'fldStatus', 'fldDisplay', 'fldComment']

    if table == 'tblSysJob':
        fields = ['fldId', 'fldJobName', 'fldJobGroup', 'fldTriggerName', 'fldTriggerGroup',
                  'fldCronExpression', 'fldIntervalInSeconds', 'fldRepeatCount',
                  'fldTriggerType', 'fldJobClass', 'fldStatus', 'fldExecuteCount']
    sqlData = processXml(xmldoc, table, fields)
    dbWrite2(conn, table, fields, sqlData)

#conn.commit()

cursor = conn.cursor()
cursor.execute('SELECT fldName from tblnode')

for record in cursor.fetchall():
    print '%s' % (record[0])
