# from MockupDB import collection

# a = collection("test", "D:", jsonSize=2)
# print(a.config["jsonAvailable"])
# a.addDoc({
#     "name" : "Dui",
#     "age" : 20
# })
# print(a.config["jsonAvailable"])
# a.addDoc({
#     "name" : "Billy",
#     "age" : 16
# })
# print(a.config["jsonAvailable"])
# a.addDoc({
#     "name" : "Boom",
#     "age" : 25
# })

# print(a.config["jsonAvailable"])
# print(a.where("age", "<", 20))
# a.deleteDoc(a.where("age", "<", 20)[0]["ID"])
# print(a.config["jsonAvailable"])

# a.addDoc({
#     "name" : "Park",
#     "age" : 20
# })
# print(a.config["jsonAvailable"])
# a.addDoc({
#     "name" : "Program",
#     "age" : 20
# })
# print(a.config["jsonAvailable"])

# print(a.where("age", "==", 25))

import json
import requests as rq
import random as rd

for i in range(50000):
    data = {
        "name" : f"JackD{i}",
        "age" : rd.randint(18,40),
        "years" : rd.randint(1,4)
    }
    rq.post("http://127.0.0.1:8000/student", json.dumps(data))
# print(rq.get("http://127.0.0.1:8000/student").json())

# from MockupDB import QueueDict

# q = QueueDict()

# q.enQueue("1", {
#     "data" : "124867454"
# })
# q.enQueue("2", {
#     "data" : "41186"
# })

# q.enQueue("3", {
#     "data" : "74747474"
# })

# print(q)

# print(q.deQueue())
# print(q)
# print(q.deQueue())
# print(q)
# print(q.deQueue())
# print(q)