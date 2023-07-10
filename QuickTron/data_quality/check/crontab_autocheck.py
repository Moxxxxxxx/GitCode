import requests, datetime, math, threading

quarter = str(datetime.datetime.now().year)+"Q"+str(math.ceil(datetime.datetime.now().month/3.))
url = "http://localhost:8080/check/rule_execute"

def post_rule_execute(productname, quarter):
    data = {'productname': productname, 'username': 'crontab', 'quarter': quarter}
    r = requests.post(url, data)

t1 = threading.Thread(target=post_rule_execute, args=('product1', quarter))
t2 = threading.Thread(target=post_rule_execute, args=('product2', quarter))


t1.start();t2.start()

# 等待运行结束
t1.join();t2.join()

