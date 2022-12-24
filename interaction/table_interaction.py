from interaction.mysql_interaction import TestPlatform, logging
from loguru import logger
import os
import datetime


path01 = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
path02 = f'log/mysql_{datetime.date.today()}.txt'
log_path = os.path.join(path01, path02)
logger.add(sink=log_path, rotation="00:00", encoding="utf-8")



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


class Step(TestPlatform):

    def __init__(self):
        super(Step, self).__init__()
        self.tableName = 'step'
        self.tableFields = {
                                'id': 'int',
                                'stepId': 'str',
                                'stepName': 'str',
                                'ruleId': 'int',
                                'upStepId': 'str',
                                'downStepId': 'str',
                                'eliminate': 'int',
                                'version': 'int'
                            }

    @logging("查询step数据")
    def query_rule(self, keyWord=None, tableField='*'):
        tableField = [item for item in self.tableFields] if tableField == '*' else tableField
        return self.query(tableName=self.tableName, tableField=tableField, keyWord=keyWord)

    @logging("添加一条step数据")
    def add_rule(self, data):
        return self.addOnce(self.tableName, data)

    @logging("修改一条step数据")
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

    step = Step()
    print(step.getFieldDefaultValue('step'))

