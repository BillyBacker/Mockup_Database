import json
import os
import uuid
from os import path
from multiprocessing import Pool
from functools import lru_cache

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

# class QueueCache:
#     def __init__(self, maxLength):
#         self.items = {}
#         self.maxLength = maxLength

#     def __len__(self):
#         return len(self.items)
#     def __str__(self):
#         return str(self.items)
#     def __getitem__(self, key):
#         item = self.items[key]
#         del self.items[key]
#         self.enQueue(key, item)
#         return self.items[key]
#     def lastKey(self):
#         return list(self.items.keys())[0]
#     def enQueue(self, key, data):
#         self.items[key] = data
#         if len(self.items) > self.maxLength:
#             self.deQueue()
#     def deQueue(self):
#         if len(self.items) != 0:
#             res = {self.lastKey() : self.items[self.lastKey()]}
#             del self.items[self.lastKey()]
#             return res
#         return {}
#     def peek(self):
#         return self.items[self.lastKey()]
#     def size(self):
#         return sys.getsizeof(self.items)
#     def available(self, key):
#         isContain = self.items[key] is not None 
#         if isContain:
#             item = self.items[key]
#             del self.items[key]
#             self.enQueue(key, item)
#         return isContain
#     def update(self, key, data):
#         self.items[key] = data

class collection:
    def __init__(self, name, repoPath, jsonSize=100, threadSize=10, CacheLength=100):
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
                "CacheLength" : CacheLength,
                "jsonAvailable" : stack(),
                "allJson" : {}
                }
            self.saveConfig()
        # self.cache = QueueCache(self.config["CacheLength"])
    def __len__(self):
        buffer = 0
        for c in self.config["allJson"].keys():
            buffer += self.config["allJson"][c]
        return buffer
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
        data = json.load(open(f"{self.collectionPath()}/{jsonName}.json", "r"))
        # self.cache.enQueue(jsonName, data)
        return data
        
    def saveJson(self, jsonName, data):
        f = open(f"{self.collectionPath()}/{jsonName}.json", "w+")
        f.write(json.dumps(data))
        f.close()
        # if self.cache.available(jsonName):
        #     self.cache.update(jsonName, data)
    def deleteJson(self, jsonName):
        fullPath = f"{self.collectionPath()}/{jsonName}.json"
        if os.path.isfile(fullPath):
            os.remove(fullPath)
    @lru_cache
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
            # if self.cache.available(jsonName):
            #     data = self.cache[jsonName]
            #     return jsonName, data
            # else:
            # data = json.load(open(f"{self.collectionPath()}/{jsonName}.json", "r"))
            # self.cache.enQueue(jsonName, data)
            return jsonName, self.loadJson(jsonName) 
    def dumpJson(self):
        res = {}
        for jsonName in self.config["allJson"].keys():
            Json = self.loadJson(jsonName)
            for docID in Json.keys():
                res[jsonName] = {"docID" : docID, "Data":Json[docID]}
        return res
    def addDoc(self, data):
        jsonName, jsonData = self.getJson()
        ID = str(uuid.uuid1())+f"_{jsonName}"
        jsonData[ID] = data
        jsonData[ID]["ID"] = ID
        self.saveJson(jsonName, jsonData)
        self.config["allJson"][jsonName] += 1
        if self.config["allJson"][jsonName] >= self.config["jsonSize"]:
            self.config["jsonAvailable"].pop()
        self.saveConfig()
    def searchThread(self, Json, operator, column, value):
        Bin = []
        if operator == "==":
            for docID in Json.keys():
                if Json[docID][column] == value:
                    Bin.append(Json[docID])
        elif operator == ">=":
            for docID in Json.keys():
                if Json[docID][column] >= value:
                    Bin.append(Json[docID])
        elif operator == "<=":
            for docID in Json.keys():
                if Json[docID][column] <= value:
                    Bin.append(Json[docID])
        elif operator == ">":
            for docID in Json.keys():
                if Json[docID][column] > value:
                    Bin.append(Json[docID])
        elif operator == "<":
            for docID in Json.keys():
                if Json[docID][column] < value:
                    Bin.append(Json[docID])
        elif operator == "!=":
            for docID in Json.keys():
                if Json[docID][column] != value:
                    Bin.append(Json[docID])
        elif operator == "contain":
            for docID in Json.keys():
                if value in Json[docID][column]:
                    Bin.append(Json[docID])
        elif operator == "#":
            for docID in Json.keys():
                    Bin.append(Json[docID])
        return Bin
    def where(self, column, operator, value):           
        res = []
        buffer = []
        pool = Pool(processes = self.config["threadSize"] if self.config["jsonAvailable"].size() > self.config["threadSize"] else self.config["jsonAvailable"].size() if self.config["jsonAvailable"].size() != 0 else 1)  
        for jsonName in self.config["allJson"].keys():
            if self.config["allJson"][jsonName] == 0:
                continue
            Json = self.loadJson(jsonName)
            buffer.append(pool.apply_async(self.searchThread, [Json, operator, column, value]))
            # t = multiprocessing.Process(target=self.searchThread, args=(Json, res, operator, column, value))
            # t.start()
            # threads.append(t)
            # while len(threads) >= self.config["threadSize"]:
            #     continue
                # garbage = []
                # for threadInd in range(len(threads)):
                #     if not threads[threadInd].is_alive():
                #         garbage.append(threadInd)
                # for threadInd in garbage:
                #     del threads[threadInd]
        # for thread in threads:
        #     thread.join()
        for ticket in buffer:
            result = ticket.get(timeout=10)
            if len(result) != 0:
                res += result
        return res
    def getDoc(self, docID):
        jsonName = docID.split("_")[1]
        return self.loadJson(jsonName)

    def findToDeleteThread(self, Json, DocId, jsonName):
            for Id in Json.keys():
                if DocId == Id:
                    print(f"Found : {Id}")
                    return [jsonName, Id]
            return []
    def deleteDoc(self, docID):
        # deleteList = []
        # buffer = []
        # pool = Pool(processes=self.config["threadSize"])
        # for jsonName in self.config["allJson"].keys():
        #     if self.config["allJson"][jsonName] == 0:
        #         continue
        #     print(f"Searching in {jsonName}.json")
        #     Json = self.loadJson(jsonName)
        #     # threads.append(multiprocessing.Process(target=self.findToDeleteThread, args=(Json, docID, deleteList, jsonName)))
        #     # threads[-1].start()
        #     buffer.append(pool.apply_async(self.findToDeleteThread, [Json, docID, jsonName]))
        # # for thread in threads:
        # #     thread.join()
        # for ticket in buffer:
        #     result = ticket.get(timeout=1)
        #     if result != None and len(result) != 0:
        #         deleteList = result
        jsonName = docID.split("_")[1]
        Json = self.loadJson(jsonName)
        print(f"Deleting : {docID}")
        del Json[docID]
        self.config["allJson"][jsonName] -= 1
        if jsonName not in self.config["jsonAvailable"].items:
            self.config["jsonAvailable"].push(jsonName)
        self.saveJson(jsonName, Json)
        self.saveConfig()
        
class MockupDB:
    def __init__(self):
        self.colume = []