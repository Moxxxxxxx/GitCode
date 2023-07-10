from django.shortcuts import render

from utils.functions import is_login
from utils import functions as f


@is_login
def dashboard(request):
    """集团仪表盘
    说明：本模块分3部分在前端展示
    1. 第一行为3个数据概览统计
    2. 第二行统计各个公司数据质量问题概况
    3. 第三行使用pyecharts做的数据统计图
    """
    return render(request, "data/dashboard.html", {"username": request.session['username']})


@is_login
def dashboard_subproductname(request):
    """
    说明：本模块分3部分在前端展示
    1. 子公司问题数据项的分布
    2. 子公司问题数据项的排序报表
    3. 改造进度
    """
    productname = request.GET.get('productname')
    productname_zh = request.GET.get('productname')
    return render(request, "data/dashboard_subproductname.html", {"productname": productname,
                                                              "productname_zh": productname_zh,
                                                              })


@is_login
def result_detail(request):
    """
    检核结果Excel明细
    :param request:
    :return:
    """
    return render(request, "data/result_detail.html", {"productname": request.GET.get('productname')})


@is_login
def report(request):
    """
    检核报告Word
    :param request:
    :return:
    """
    return render(request, "data/report.html")