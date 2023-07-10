#结果校验
import logging

try:
    logger = logging.getLogger()
except:
    print('logging.getLogger error')

from utils import fn_setting


class Result_Check(object):

    def __init__(self):
       pass

    def verify_result(self, origin, operator, expect, check_result_list=None):
        self.verify_result = False
        try:
            if origin or expect:
                if origin and isinstance(origin, str):
                    origin = origin.strip()
                if expect and isinstance(expect, str):
                    expect =expect.strip()
                if str(origin).isdigit():
                    origin = int(origin)
                if str(expect).isdigit():
                    expect = int(expect)
                if operator == '=':
                    self.verify_result = (origin == expect)
                elif operator == '!=':
                    if isinstance(origin, str):
                        self.verify_result = origin.find(expect) < 0
                    else:
                        self.verify_result = (origin != expect)
                elif operator == '>':
                    self.verify_result = (origin > expect)
                elif operator == '>=':
                    self.verify_result = (origin >= expect)
                elif operator == '<':
                    self.verify_result = (origin < expect)
                elif operator == '<=':
                    self.verify_result = (origin <= expect)
                elif operator == 'include':
                    self.verify_result = (str(origin).find(str(expect)) > -1)
                elif operator.find('()') > -1: #function
                    fnstatement = f"self.verify_result = fn_setting.{operator[:-2]}('{origin}', '{expect}')"
                    logger.info('exec statement: ', fnstatement)
                    exec(fnstatement)
                else:
                    logger.info('sorry,不可以识别的操作符')
                    self.verify_result = False
            else:
                if (not origin and not expect) or (str(origin) == str(expect)):
                    self.verify_result = True
                else:
                    logger.info('err')
        except Exception as e:
            logger.error("verify_result error")
            logger.error(e)
        return self.verify_result

if __name__ == '__main__':
    s = Result_Check().verify_result('tes343434', 'test1()', 'tes343434')
    print(s)
