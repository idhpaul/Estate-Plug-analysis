#%%
# conda install pymongo
# 애드온 설치 : MongoDB for VS Code

import datetime
from pymongo import MongoClient 

my_client = MongoClient("mongodb://root:P%40ssw0rd@52.79.55.128:27017/?authSource=admin&readPreference=primary&ssl=false")

print(my_client.list_database_names())



#mydb = my_client['estate-plug'] 
#mycol = mydb['apart_trade_kangnam_indecrease']

#%%
collection  = my_client.apart_trade_count

#%%
# find Data
result = mycol.find()
for i in result:
    print(i)

#%%
x = mycol.insert_one({"year_month":"1월", "increase":0, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})

print(x.inserted_id)

#%%
# Delete Data
mycol.delete_one({
    "year_month": "1월"
})  

#%%
# All data remove
mycol.remove({})
