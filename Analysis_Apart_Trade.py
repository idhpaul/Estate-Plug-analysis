#%%
# Init S3 Env
# conda install pylint

import datetime
import sys
import pandas as pd

import matplotlib as mpl           
import matplotlib.pyplot as plt
from matplotlib import font_manager, rc

import boto3
import botostubs

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO       # Python 3.x

from pymongo import MongoClient             # for MongoDB

#%matplotlib inline
font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)
plt.rcParams['axes.unicode_minus'] = False

"""
*******************************************************************************
* Connect_MongoDB()                     MongDB 접속

* @return                   <mydb>      MogoDB 반환
*******************************************************************************
"""
def Connect_MongoDB():

    my_client = MongoClient("mongodb://root:P%40ssw0rd@52.79.55.128:27017/?authSource=admin&readPreference=primary&ssl=false")

    print(my_client.list_database_names())

    mydb = my_client['estate-plug'] 
    return mydb

"""
*******************************************************************************
* Init_S3Client()                       S3 Client 생성

* @return                   <client>    S3 Client 반환
*******************************************************************************
"""
def Init_S3Client():
    AWS_ACCESS_KEY_ID ="AKIAT4IAQPVVMMBS2FNX"
    AWS_SECRET_ACCESS_KEY = "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"
    AWS_DEFAULT_REGION = "ap-northeast-2"
    client = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_DEFAULT_REGION
                        ) # type: botostubs.S3

    return client

"""
*******************************************************************************
* Read_Trade()                          S3 CSV파일 로드

* @param [in]               <_s3Obj>    S3 Client / Init_S3Client() 로드 필수
* @param [in | str]         <_item>     Target context ("aprat", "land"..)
* @param [in | str]         <_year>     Target year ("2015"~)
* @param [in | str]         <_month>    Target month ("01"~"12")

* @return                   <df>        item, year, month에 해당 csv DataFrame 데이터
*******************************************************************************
"""
def Read_Trade(_s3Obj, _item, _year, _month):

    s3FileName = _item + "_trade_" + _year + _month + ".csv"
    print(s3FileName)

    csv_obj = _s3Obj.get_object(Bucket='store-estate', Key=s3FileName)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))

    # Drop Unnecessary Columns 
    df = df.drop(df.columns[0], axis='columns')

    return df

"""
*******************************************************************************
* Count_Trade()                            지역코드별 Row 개수

* @param [in]               <dataframe>    CSV Dataframe Data

* @return                   <df>           지역코드별 Row 개수 DataFrame
*******************************************************************************
"""
def Count_Trade(dataframe):
    seriesData = dataframe['지역코드'].value_counts().sort_index()
    if '지역코드' in seriesData:
        seriesData = seriesData.drop('지역코드')        # Filfltering

    seriesData.index = seriesData.index.astype(int)

    df = seriesData.to_frame()
    return df

# def Count_Trade(dataframe):
#     seriesData = dataframe['지역코드'].value_counts().sort_index()
#     if '지역코드' in seriesData:
#         seriesData = seriesData.drop('지역코드')        # Filfltering

#     seriesData.index = seriesData.index.astype(int)

#     df = seriesData.to_frame()
#     return df

def Target_Area_Trade_Increase_Decrease(_targetItem, _targetYear, _targetBeginMonth, _targetEndMonth,  _targetAreadCode):
    
    target_trade_count_list = []
    diff_before_target_trade_count_list = [];

    client = Init_S3Client()

    for monthStr in range(_targetBeginMonth, _targetEndMonth + 1):

        if monthStr < 10:
            monthStr = "0" + str(monthStr)
        else:
            monthStr = str(monthStr)

        df = Read_Trade(_s3Obj= client, 
                        _item= _targetItem, 
                        _year= str(_targetYear), 
                        _month= monthStr)

        filterCountTrade = Count_Trade(df)

        target_trade_count_list.append(filterCountTrade)

        beforeIdx = 0
        currentIdx = 0

    # for i in target_trade_count_list:
    #     print(diffDF, i)

    
    
# %%
# 지역코드별 거래량 그래프

'''
Needed
    - 지역코드
    - 년/월 별 Dataframe
    - 년/월 별 Dataframe 거래량
    - 각 거래량을 기반으로 그래프 작성
'''
# item = "apart"
# areaCode = 11110
# year = 2015
# beginMonth = 1
# endMonth = 12


# Target_Area_Trade_Increase_Decrease(targetItem= item, 
#                                     targetYear= year, 
#                                     targetBeginMonth= beginMonth, 
#                                     targetEndMonth= endMonth,
#                                     targetAreadCode= areaCode )

client = Init_S3Client()

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="01")
dfTC201501=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="02")
dfTC201502=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="03")
dfTC201503=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="04")
dfTC201504=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="05")
dfTC201505=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="06")
dfTC201506=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="07")
dfTC201507=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="08")
dfTC201508=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="09")
dfTC201509=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="10")
dfTC201510=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="11")
dfTC201511=Count_Trade(df)

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="12")
dfTC201512=Count_Trade(df)

# 전월별 거래량 차이
diff0102 = dfTC201502 - dfTC201501
diff0203 = dfTC201503 - dfTC201502
diff0304 = dfTC201504 - dfTC201503
diff0405 = dfTC201505 - dfTC201504
diff0506 = dfTC201506 - dfTC201505
diff0607 = dfTC201507 - dfTC201506
diff0708 = dfTC201508 - dfTC201507
diff0809 = dfTC201509 - dfTC201508
diff0910 = dfTC201510 - dfTC201509
diff1011 = dfTC201511 - dfTC201510
diff1112 = dfTC201512 - dfTC201511

#%%
# 특정 지역코드 거래량 차이

#강남구 11680
#강동구 11740
#관악구 11620
#구로구 11530
#종로구 11110

targetAreaCode = 11680

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])


def Insert_apart_trade_targetarea_increase(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_kangnam_indecrease"]
    x = myCol.insert_one({"target_area_code":target_area_codeData, "year_month":year_monthData, "increase":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)


s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
Insert_apart_trade_targetarea_increase(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 강남구 거래량 차이

#강남구 11680

targetAreaCode = 11680

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])

def apart_trade_kangnam_indecrease(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_kangnam_indecrease"]
    x = myCol.insert_one({"area":"강남구", "year_month":year_monthData, "indecrease":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)

s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
apart_trade_kangnam_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 강동구 거래량 차이

#강동구 11740

targetAreaCode = 11740

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])

def apart_trade_kangdong_indecrease(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_kangdong_indecrease"]
    x = myCol.insert_one({"area":"강동구", "year_month":year_monthData, "indecrease":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)

s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
apart_trade_kangdong_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 관악구 거래량 차이

#관악구 11620

targetAreaCode = 11620

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])

def apart_trade_gwanak_indecrease(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_gwanak_indecrease"]
    x = myCol.insert_one({"area":"관악구", "year_month":year_monthData, "indecrease":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)

s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
apart_trade_gwanak_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 구로구 거래량 차이

#구로구 11530

targetAreaCode = 11530

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])

def apart_trade_guro_indecrease(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_guro_indecrease"]
    x = myCol.insert_one({"area":"구로구", "year_month":year_monthData, "indecrease":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)

s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
apart_trade_guro_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 종로구 거래량 차이

#종로구 11110

targetAreaCode = 11110

print(diff0102.loc[targetAreaCode])
print(diff0203.loc[targetAreaCode])
print(diff0304.loc[targetAreaCode])
print(diff0405.loc[targetAreaCode])
print(diff0506.loc[targetAreaCode])
print(diff0607.loc[targetAreaCode])
print(diff0708.loc[targetAreaCode])
print(diff0809.loc[targetAreaCode])
print(diff0910.loc[targetAreaCode])
print(diff1011.loc[targetAreaCode])
print(diff1112.loc[targetAreaCode])


def apart_trade_jongno_indecrease(target_area_codeData,year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_jongno_indecrease"]
    x = myCol.insert_one({"area":"종로구", "year_month":year_monthData, "indecrease":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)

s1 = pd.DataFrame({"증감량":[]})

data = 0.0
index = "2015_01"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0102.loc[targetAreaCode]['지역코드']
index = "2015_02"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0203.loc[targetAreaCode]['지역코드']
index = "2015_03"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0304.loc[targetAreaCode]['지역코드']
index = "2015_04"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0405.loc[targetAreaCode]['지역코드']
index = "2015_05"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0506.loc[targetAreaCode]['지역코드']
index = "2015_06"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0607.loc[targetAreaCode]['지역코드']
index = "2015_07"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0708.loc[targetAreaCode]['지역코드']
index = "2015_08"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0809.loc[targetAreaCode]['지역코드']
index = "2015_09"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff0910.loc[targetAreaCode]['지역코드']
index = "2015_10"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1011.loc[targetAreaCode]['지역코드']
index = "2015_11"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

data = diff1112.loc[targetAreaCode]['지역코드']
index = "2015_12"
s1.loc[index] = [data]
apart_trade_jongno_indecrease(target_area_codeData=targetAreaCode,year_monthData= ''.join(index), increaseData= data)

s1.plot()

#%%
# 지역코드별 평균 거래 금액 그래프
'''
Needed
    - 지역코드
    - 년/월 별 Dataframe
    - 년/월 별 Dataframe 평균 거래 금액
    - 각 평균 거래 금액을 기반으로 그래프 작성
'''
# %%
startMonth = 1;
endMonth = 12;

for target_list in range(1,endMonth + 1):
    if target_list < 10:
        target_list = "0" + str(target_list)
    else:
        target_list = str(target_list)
    
    print(target_list)

#%%

def Insert_apart_trade_count(year_monthData, increaseData):
    myDB = Connect_MongoDB()
    myCol = myDB["apart_trade_count"]
    x = myCol.insert_one({"year_month":year_monthData, "totaltrade":increaseData, "createdAt": datetime.datetime.now(), "updatedAT":datetime.datetime.now()})
    print(x.inserted_id)


client = Init_S3Client()

df1 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="01")
df2 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="02")
df3 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="03")
df4 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="04")
df5 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="05")
df6 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="06")
df7 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="07")
df8 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="08")
df9 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="09")
df10 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="10")
df11 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="11")
df12 = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="12")

s1 = pd.DataFrame({"증감량":[]})

data = len(df1)
index = "2015_01"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df2)
index = "2015_02"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df3)
index = "2015_03"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df4)
index = "2015_04"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df5)
index = "2015_05"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df6)
index = "2015_06"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df7)
index = "2015_07"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df8)
index = "2015_08"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df9)
index = "2015_09"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df10)
index = "2015_10"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

data = len(df11)
index = "2015_11"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)


data = len(df12)
index = "2015_12"
s1.loc[index] = [data]
Insert_apart_trade_count(year_monthData= ''.join(index), increaseData= data)

s1

s1.plot()

#%%
# TODO : 지역별 평균 거래 금액
regioncode = 11110

client = Init_S3Client()

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="01")
meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf1 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="02")
filterdf = df['지역코드'] != '지역코드'
filterdf = filterdf.astype({'지역코드': 'int64'})
#filterdf['지역코드'] = filterdf['지역코드'].astype(int64)
meandf = filterdf.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf2 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="03")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf3 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="04")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf4 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="05")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf5 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="06")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf6 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="07")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf7 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="08")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf8 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="09")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf9 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="10")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf10 = meandf[meandf['지역코드'] == regioncode]

df = Read_Trade(_s3Obj= client, _item= "apart", _year= "2015", _month="11")

meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf11 = meandf[meandf['지역코드'] == regioncode]


meandf = df.groupby(['지역코드'], as_index=False).mean()
meandf = meandf[['지역코드','거래금액']]
meandf12 = meandf[meandf['지역코드'] == regioncode]