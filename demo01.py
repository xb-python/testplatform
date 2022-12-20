from configs import *
import pymysql
from loguru import logger


class TestPlatform:

    def __init__(self):
        conn = pymysql.connect(host=HOST,port=PORT,user=USER,password=PASSWORD,database='test_platform',charset='utf8')
        self.cursor = conn.cursor()


class Rule(TestPlatform):

    def __init__(self):
        super(Rule, self).__init__()
        self.tableFields = {
                                'id': 'int',
                                'ruleName': 'str',
                                'rulePremiseId': 'int',
                                'details': 'str',
                                'eliminate': 'int',
                                'version': 'int'
                            }

    def query(self, keyWord=None, tableField='*'):
        tableField = self.tableFields.keys() if tableField == '*' else tableField
        logger.debug(keyWord)
        if keyWord:
            keyWordList = []
            for key in keyWord.keys():
                if isinstance(keyWord[key], str):
                    keyWordList.append(f'{key}="{keyWord[key]}"')
                else:
                    keyWordList.append(f'{key}={keyWord[key]}')
            keyWordStr = ' and '.join(keyWordList)
            sql = f"select {','.join(tableField)} from rule where {keyWordStr};"
        else:
            sql = f"select {','.join(tableField)} from rule;"
        logger.debug(sql)
        self.cursor.execute(sql)

        data = []
        for line in self.cursor.fetchall():
            lineData = {}
            for field in range(len(line)):
                print(line(field))
                # lineData[tableField[field]] = line(field)
            data.append(lineData)
        print(data)


if __name__ == '__main__':

    rule = Rule()
    rule.query({'id':1,'ruleName':"微众身份证影印件合规规则"})
    rule.query(tableField=['ruleName'])
