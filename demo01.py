from configs import *
import pymysql
from loguru import logger


def logging(funcDesc=None):  # 用于装饰的函数名
    def outwrapper(func):  # 接收 用于调用的函数名
        def wrapper(*args, **kwargs):  # 接收 传递到调用函数的参数
            logger.info(f"{funcDesc},调用:{func.__name__}()")
            logger.info(f"请求参数 {func.__name__}(): {kwargs}")
            returnData = func(*args, **kwargs)  # 调用被装饰的函数
            logger.debug(f"{'返回数据'} {func.__name__}():{returnData}")
            return returnData

        return wrapper

    return outwrapper


class TestPlatform:

    def __init__(self):
        conn = pymysql.connect(host=HOST,port=PORT,user=USER,password=PASSWORD,database='test_platform',charset='utf8')
        self.cursor = conn.cursor()

    @logger.catch
    @logging("查询数据")
    def query(self, tableName, tableField, keyWord=None):
        """
        查询语句 条件关系and
        :param tableName: 表名 ‘rule’
        :param keyWord: 查询条件 {'id': 1, 'ruleName': '微众身份证影印件合规规则'}
        :param tableField: 获取字段数据['id','ruleName']
        :return: data 列表嵌套字典 [{'id': 1, 'ruleName': '微众身份证影印件合规规则'}]
        """
        if keyWord:
            keyWordList = [f'{key}="{keyWord[key]}"' if isinstance(keyWord[key], str) else f'{key}={keyWord[key]}' for key in keyWord.keys()]
            keyWordStr = ' and '.join(keyWordList)
            sql = f"select {','.join(tableField)} from {tableName} where {keyWordStr};"
        else:
            sql = f"select {','.join(tableField)} from {tableName};"
        logger.debug(f'SQL：{sql}')
        self.cursor.execute(sql)

        data = []
        for line in self.cursor.fetchall():
            lineData = {}
            for field in range(len(line)):
                lineData[tableField[field]] = line[field]
            data.append(lineData)
        return data



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

    @logging("添加rule数据")
    def add_rule(self, data):
        keysList = [key for key in data.keys()]
        valueList = [value for value in data.values()]
        print(type(keysList), keysList)
        print(type(valueList), valueList)
        pass
        f"insert into students(name,age) values('王二小',15); "



if __name__ == '__main__':

    rule = Rule()
    print(rule.query_rule({'ruleName': '微众身份证影印件合规规则'}))
    print(rule.add_rule({'details': '测试'}))
