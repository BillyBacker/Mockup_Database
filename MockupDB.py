import json
import os
import uuid
from os import path

class stack:
    def __init__(self, list = None):
        if list == None:
            self.items = []
        else:
            self.items = list

    def __str__(self):
        s = 'stack of '+ str(self.size())+' items : '
        for ele in self.items:
            s += str(ele)+' '
        return s
    def parseJson(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)
    def push(self, i):
        self.items.append(i)

    def pop(self):   #edit code
        return self.items.pop()

    def peek(self):
        return self.items[-1]

    def isEmpty(self):
        return self.items == []

    def size(self):
        return len(self.items)

class collection:
    def __init__(self, name, repoPath, jsonSize=100, threadSize=10):
        def mkdir(Path, name):
            fullPath = f"{Path}/{name}"
            if path.exists(fullPath):
                return
            else:
                os.mkdir(fullPath)
        def isExits(Path, name):
            fullPath = f"{Path}/{name}"
            print(fullPath)
            return path.isfile(fullPath)
        self.config = None
        # self.name = name
        # self.repo = repoPath
        # self.jsonSize = jsonSize
        p = f"_{name}.cnfg"
        if isExits(f"{repoPath}/{name}", p):
            self.config = json.loads(open(f"{repoPath}/{name}/{p}", "r").read())
            self.config["jsonAvailable"] = stack(self.config["jsonAvailable"])
        else:
            mkdir(repoPath,name)
            self.config = {
                "name" : name,
                "path" : repoPath,
                "jsonSize" : jsonSize,
                "threadSize" : threadSize,
                "jsonAvailable" : stack(),
                "allJson" : {}
                }
            self.saveConfig()
    def saveConfig(self):
        name = self.config["name"]
        p = f"_{name}.cnfg"
        f = open(f"{self.collectionPath()}/{p}", "w")
        self.config["jsonAvailable"] = self.config["jsonAvailable"].items
        f.write(json.dumps(self.config))
        f.close()
        self.config["jsonAvailable"] = stack(self.config["jsonAvailable"])
    def doConfig(self, param, val):
        self.config[param] = val
        self.saveConfig()
    def collectionPath(self):
        name = self.config["name"]
        path = self.config["path"]
        return f"{path}/{name}"
    def createNewJson(self, jsonName):
        fullPath = f"{self.collectionPath()}/{jsonName}.json"
        if not os.path.isfile(fullPath):
            f = open(f"{self.collectionPath()}/{jsonName}.json", "x")
            f.write(json.dumps({}))
            f.close()
            self.config["jsonAvailable"].push(jsonName)
            self.config["allJson"][jsonName] = 0
            self.saveConfig()
        else:
            print(f"Warning : createNewJson skipped due to exitsing file. file : {fullPath}")
    def loadJson(self, jsonName):
        return json.load(open(f"{self.collectionPath()}/{jsonName}.json", "r"))
    def saveJson(self, jsonName, data):
        f = open(f"{self.collectionPath()}/{jsonName}.json", "w+")
        f.write(json.dumps(data))
        f.close()
    def deleteJson(self, jsonName):
        fullPath = f"{self.collectionPath()}/{jsonName}.json"
        if os.path.isfile(fullPath):
            os.remove(fullPath)
    def getJson(self):
        # for jsonName in self.config["jsonAvailable"].keys():
        #     if self.config["jsonAvailable"][jsonName] < self.config["jsonSize"]:
        #         return jsonName, self.loadJson(jsonName)
        # newName = str(len(self.config["jsonAvailable"]))
        # self.createNewJson(newName)
        # return newName, self.loadJson(newName)
        if self.config["jsonAvailable"].isEmpty():
            newName = str(len(self.config["allJson"]))
            self.createNewJson(newName)
            return newName, self.loadJson(newName) 
        else:
            jsonName = self.config["jsonAvailable"].peek()
            return jsonName, self.loadJson(jsonName) 
    def dumpJson(self):
        res = {}
        for jsonName in self.config["allJson"].keys():
            Json = self.loadJson(jsonName)
            for docID in Json.keys():
                print(type(jsonName), type(docID), type(Json[docID]))
                res[jsonName] = {"docID" : docID, "Data":Json[docID]}
        return res
    def addDoc(self, data):
        ID = uuid.uuid1()
        jsonName, jsonData = self.getJson()
        jsonData[str(ID)] = data
        self.saveJson(jsonName, jsonData)
        self.config["allJson"][jsonName] += 1
        if self.config["allJson"][jsonName] >= self.config["jsonSize"]:
            self.config["jsonAvailable"].pop()
        self.saveConfig()
    def where(self, column, operator, value):
        res = {}
        for jsonName in self.config["allJson"].keys():
            Json = self.loadJson(jsonName)
            for docID in Json.keys():
                if operator == "==":
                    if Json[docID][column] == value:
                        res[docID] = Json[docID]
                elif operator == ">=":
                    if Json[docID][column] >= value:
                        res[docID] = Json[docID]
                elif operator == "<=":
                    if Json[docID][column] <= value:
                        res[docID] = Json[docID]
                elif operator == ">":
                    if Json[docID][column] > value:
                        res[docID] = Json[docID]
                elif operator == "<":
                    if Json[docID][column] < value:
                        res[docID] = Json[docID]
                elif operator == "!=":
                    if Json[docID][column] != value:
                        res[docID] = Json[docID]
        return res
    def deleteDoc(self, docID):
        deleteList = []
        for jsonName in self.config["allJson"].keys():
            Json = self.loadJson(jsonName)
            for Id in Json.keys():
                if docID == Id:
                    deleteList.append([Id, jsonName])
        for docID, jsonName in deleteList:
            Json = self.loadJson(jsonName)
            print(f"deleting : {Json[docID]}")
            del Json[docID]
            self.saveJson(jsonName, Json)
            self.config["allJson"][jsonName] -= 1
            if jsonName not in self.config["jsonAvailable"].items:
                self.config["jsonAvailable"].push(jsonName)
        
class MockupDB:
    def __init__(self):
        self.colume = []