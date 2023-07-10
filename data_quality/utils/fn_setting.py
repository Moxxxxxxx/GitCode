#自定义方法

#如果是实现检验方法，必须是返回boolean,调用将传2个参数orign, expect
def test(orign, expect):
    print('test')
    return True

def test1(orign, expect):
    return orign.find(expect) > -1
