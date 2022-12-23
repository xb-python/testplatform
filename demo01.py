from configs import *
import pymysql
from loguru import logger


def logging(funcDesc=None):  # 用于装饰的函数名
    def outwrapper(func):  # 接收 用于调用的函数名
        def wrapper(*args, **kwargs):  # 接收 传递到调用函数的参数
            logger.info(f"{funcDesc},调用:{func.__name__}()")
            logger.info(f"请求参数 {func.__name__}(): {args,kwargs}")
            returnData = func(*args, **kwargs)  # 调用被装饰的函数
            logger.debug(f"{'返回数据'} {func.__name__}():{returnData}")
            return returnData

        return wrapper

    return outwrapper


class TestPlatform:

    def __init__(self):
        self.db = pymysql.connect(host=HOST,port=PORT,user=USER,password=PASSWORD,database='test_platform',charset='utf8')
        self.cursor = self.db.cursor()

    @logger.catch
    @logging("获取所有字段名及默认值")
    def getFieldDefaultValue(self, tableName):
        """
        获取所有字段名及默认值
        :param tableName: 表名
        :return:
        """
        # 查询 默认
        sql = f"select COLUMN_NAME,COLUMN_TYPE  from information_schema.COLUMNS where table_name = '{tableName}';"
        self.cursor.execute(sql)
        fieldsdefaultValue = {}     # 存储 默认值
        for line in self.cursor.fetchall():
            if line[0] == 'id':
                continue
            fieldsdefaultValue[line[0]] = None if 'varchar' in line[1] else 0
        return fieldsdefaultValue

    @logger.catch
    @logging("查询数据")
    def query(self, tableName, tableField, keyWord=None):
        """
        查询数据 条件关系and
        :param tableName: 表名
        :param keyWord: 查询条件 {'id': 1, 'ruleName': '微众身份证影印件合规规则'}
        :param tableField: 获取字段数据['id','ruleName']
        :return: dataList 列表嵌套字典 [{'id': 1, 'ruleName': '微众身份证影印件合规规则'}]
        """
        if keyWord:
            keyWordList = [f'{key}="{keyWord[key]}"' if isinstance(keyWord[key], str) else f'{key}={keyWord[key]}' for key in keyWord.keys()]
            keyWordStr = ' and '.join(keyWordList)
            sql = f"select {','.join(tableField)} from {tableName} where {keyWordStr};"
        else:
            sql = f"select {','.join(tableField)} from {tableName};"
        logger.debug(f'SQL：{sql}')
        self.cursor.execute(sql)

        dataList = []
        for line in self.cursor.fetchall():
            lineData = {}
            for field in range(len(line)):
                lineData[tableField[field]] = line[field]
            dataList.append(lineData)
        return dataList

    @logger.catch
    @logging("单挑添加数据")
    def addOnce(self, tableName, data):
        """
        写入数据
        :param tableName: 表名
        :param data: data 字典 {'id': 1, 'ruleName': '微众身份证影印件合规规则'}
        :return:
        """
        keysList = [key for key in data.keys()]
        valueList = [f'"{value}"' if isinstance(value, str) else value for value in data.values()]
        sql = f"insert into {tableName}({','.join(keysList)}) values({','.join(valueList)});"
        logger.debug(f'SQL：{sql}')
        self.cursor.execute(sql)
        self.db.commit()
        for line in self.cursor.fetchall():
            print('line:', line)

    @logger.catch
    @logging("批量添加数据")
    def addBatch(self, tableName, dataList):
        """

        :param tableName: 表名
        :param dataList:列表嵌套字典 [{'id': 1, 'ruleName': '微众身份证影印件合规规则'}]
        :return:
        """
        # 查询 默认值，补全空字段

        defaultValueList = []
        for data in dataList:
            defaultValue = [data[key] for key in data.keys()]
            defaultValueList.append(defaultValue)
        print(defaultValueList)

        # INSERT INTO table_name  (field1, field2,...fieldN)  VALUES  (valueA1,valueA2,...valueAN),(valueB1,valueB2,...valueBN),(valueC1,valueC2,...valueCN)......;


        pass


class Rule(TestPlatform):

    def __init__(self):
        super(Rule, self).__init__()
        self.tableName = 'rule'
        self.tableFields = {
                                'id': 'int',
                                'ruleName': 'str',
                                'rulePremiseId': 'int',
                                'details': 'str',
                                'eliminate': 'int',
                                'version': 'int'
                            }

    @logging("查询rule数据")
    def query_rule(self, keyWord=None, tableField='*'):
        tableField = [item for item in self.tableFields] if tableField == '*' else tableField
        return self.query(tableName=self.tableName, tableField=tableField, keyWord=keyWord)

    @logging("添加一个rule数据")
    def add_rule(self, data):
        return self.addOnce(self.tableName, data)

    @logging("批量添加rule数据")
    def adds_rule(self, dataList):
        return self.addBatch(self.tableName, dataList)


if __name__ == '__main__':

    rule = Rule()
    # print(rule.query_rule({'ruleName': '微众身份证影印件合规规则'}))
    # print(rule.query_rule())
    # print(rule.add_rule({'ruleName': '测试规则'}))
    dataList = [{'ruleName': '微众身份证影印件合规规则'}, {'ruleName': '零钱提额用户的二类卡日剩余额度规则',}]
    rule.adds_rule(dataList)
