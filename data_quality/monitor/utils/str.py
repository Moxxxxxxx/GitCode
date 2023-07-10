# -*- encoding: utf-8 -*-
"""
@File    :   str.py    
@Contact :   puyongjun@flashhold.com
@License :   (C)Copyright 2021-2025

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/8/5 13:28   parker      1.0         None
"""
import datetime
import hashlib
import re
import difflib


def str_to_time(st: str, _format="%Y-%m-%d %H:%M:%S"):
    """
    字符串格式化为时间
    :param st:
    :param _format:
    :return:
    """
    return datetime.datetime.strptime(st, _format)


def gen_md5(value):
    """
    根据输入值编码为 md5
    :param value: 输入值
    :return:
    """
    return hashlib.md5(value.encode(encoding='UTF-8')).hexdigest()


def str_is_compile(value, compile_str) -> bool:
    """
    匹配一个字符串是否包含某个正则
    :param value:
    :param compile_str:
    :return:
    """
    r = []
    try:
        r = re.findall(compile_str, value)
    except Exception as e:
        print(e)
    return True if len(r) >= 1 else False


def string_similar(s1: str, s2: str) -> float:
    """
    比较两个字符串的相似度
    :param s1:
    :param s2:
    :return:
    """
    return difflib.SequenceMatcher(None, s1, s2).quick_ratio()
