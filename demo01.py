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

        # 获取最新的一条数据
        sql = f'SELECT * FROM {tableName} ORDER BY id DESC LIMIT 1;'
        self.cursor.execute(sql)
        for line in self.cursor.fetchall():
            return line

    @logger.catch
    @logging("修改一条数据")
    def change(self, tableName, targetData, changeData):
        """
        修改数据
        :param tableName: 表名
        :param targetData: 字典：用于确定要修改哪条数据 {'id': 1, 'version': 1}
        :param changeData: 字典：修改数据 {'ruleName': '微众身份证影印件合规规则', 'rulePremiseId': 0}
        :return:
        """
        # {'code': 0, 'message': 'success', 'data': []}
        # {'code': -1, 'message': 'failure'}

        changeData['version'] = targetData['version'] + 1
        targetDataList = [f'{key}="{targetData[key]}"' if isinstance(targetData[key], str) else f'{key}={targetData[key]}' for
                       key in targetData.keys()]
        changeDataList = [f'{key}="{changeData[key]}"' if isinstance(changeData[key], str) else f'{key}={changeData[key]}' for
                       key in changeData.keys()]
        getSql = f"select * from {tableName} where {' and '.join(targetDataList)};"
        self.cursor.execute(getSql)
        if len([line for line in self.cursor.fetchall()]) != 1:
            return {'code': -1, 'message': '未查询到要修改的数据'}

        sql = f"UPDATE {tableName} SET {','.join(changeDataList)} WHERE {' and '.join(targetDataList)};"
        logger.debug(f'SQL：{sql}')
        self.cursor.execute(sql)
        self.db.commit()

        getSql = f"select * from {tableName} where id={targetData['id']};"
        self.cursor.execute(getSql)
        for line in self.cursor.fetchall():
            return {'code': 0, 'message': 'success', 'data': line}



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

    @logging("添加一条rule数据")
    def add_rule(self, data):
        return self.addOnce(self.tableName, data)

    @logging("修改一条rule数据")
    def change_rule(self, targetData, changeData):
        """

        :param targetData: 字典：用于确定要修改哪条数据 {'id': 1, 'version': 1}
        :param changeData: 字典：修改数据 {'ruleName': '微众身份证影印件合规规则', 'rulePremiseId': 0}
        :return:
        """
        # {'code': 0, 'message': 'success', 'data': []}
        # {'code': -1, 'message': 'failure'}
        return self.change(self.tableName, targetData, changeData)


if __name__ == '__main__':

    rule = Rule()
    # print(rule.query_rule({'ruleName': '游戏规则'}))
    # print(rule.query_rule())
    # print(rule.add_rule({'ruleName': '游戏规则'}))
    print(rule.change_rule({'id': 1, 'version': 3},{'ruleName': '游戏规则12123'}))

