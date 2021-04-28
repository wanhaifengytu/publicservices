# -*- coding: utf-8 -*-
from py2neo import Node, Graph, Relationship,NodeMatcher

import os
import pandas as pd
import json

from parsePerflogCallStack import  *


def connectToNeo4j(url, user, password):
    """建立连接"""
    link = Graph(url, auth=(user, password))

    return link

def deleteAll(link):
    link.delete_all()

def createNode(link, label, name):
    node = Node(label, name=name)
    link.create(node)
    return node

def createRelation(link, nodeA, nodeB):
    relationship = Relationship(nodeA, nodeB)
    link.create(relationship)


#read huge csv files per lines(chunkSize)
#['_time', 'host', 'CMID', 'UID', 'EID', 'GID', 'MTD', 'URL', 'RQT',
# 'MID', 'PID', 'PQ', 'MEM', 'CPU', 'UCPU', 'SQLC', 'SQLT', 'STK']
def readPerflogCsv(csvPath,chunkSize=20):
    for chunkPd in pd.read_csv(csvPath, chunksize=chunkSize):
        print("columns", chunkPd.columns)
        jsonSTKs = chunkPd["STK"]
        listSTKs = jsonSTKs.tolist()
        for stkEach in listSTKs:
            stkData = ""
            try:
                stkData = json.loads(stkEach)
                print("will parse ", stkData)
                parsePerfLogCallstack(stkData)

            except ValueError:
                print('Decoding JSON failed for ', stkEach)
                continue
            break   #temp
        break       # temp

def matchByLabelName(link, label, name):
    nodes = NodeMatcher(link)
    matchedNode = nodes.match(label, name=name).first()
    return matchedNode



if __name__ == '__main__':
    print("Start to run main of read perflog")
    url = "http://localhost:7474"
    user = "neo4j"
    password = "tangyudiadid0"
    graphName = "mygraph"
    link = connectToNeo4j(url, user, password)
    deleteAll(link)

    aliceNode = createNode(link, "person", "Alice")
    bobNode = createNode(link, "person", "Bob")

    createRelation(link, aliceNode, bobNode)


    readPerflogCsv("./April21_10mins.csv")







