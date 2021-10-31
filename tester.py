from MockupDB import collection

a = collection("test", "D:", jsonSize=2)
print(a.config["jsonAvailable"])
a.addDoc({
    "name" : "Dui",
    "age" : 20
})
print(a.config["jsonAvailable"])
a.addDoc({
    "name" : "Billy",
    "age" : 16
})
print(a.config["jsonAvailable"])
a.addDoc({
    "name" : "Boom",
    "age" : 25
})

print(a.config["jsonAvailable"])
a.deleteDoc(list(a.where("age", "<", 20).keys())[0])
print(a.config["jsonAvailable"])

a.addDoc({
    "name" : "Park",
    "age" : 20
})
print(a.config["jsonAvailable"])
a.addDoc({
    "name" : "Program",
    "age" : 20
})
print(a.config["jsonAvailable"])

print(a.where("age", "==", 25))