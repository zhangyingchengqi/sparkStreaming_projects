#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import jieba
cut=jieba.cut
from bottle import route,run

#利用结巴分词的分词的函数
def token(sentence):
    seg_list=list(cut(sentence))
    return " ".join(seg_list)

#路由设置
@route( '/token/:sentence')
def index(sentence):
    result=token( sentence)
    return "{\"ret\":0, \"msg\":\"OK\",\"terms\":\"%s\"}" % result

if __name__=="__main__":
    #以      localhost:8282/token/今天是星期天       访问
    run( host="localhost",port=8282)