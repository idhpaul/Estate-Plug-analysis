#%%
# Init S3 Env
# conda install pylint

import datetime
from re import A
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
* Connect_MongoDB()                     

* @brief                                        MongDB 접속

* @return   MongoClient                         MogoDB 반환
*******************************************************************************
"""
def Connect_MongoDB():

    my_client = MongoClient("mongodb://root:P%40ssw0rd@52.79.55.128:27017/?authSource=admin&readPreference=primary&ssl=false")

    print(my_client.list_database_names())

    mydb = my_client['estate-plug'] 
    return mydb

"""
*******************************************************************************
* Init_S3Client()                               

* @brief                                        S3 Client 생성

* @return   botostubs.S3                        S3 Client 반환
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
* Read_Trade(_s3Obj, _item, _year, _month) 

* @brief                                        Read Target item

* @param [in] botostubs.S3  _s3Obj              S3 Client / Init_S3Client() 로드 필수
* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] string        _year               Target item year
* @param [in] string        _month              Target item month

* @return     DataFrame                         Return target month for the year DataFrame  
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
    
    if (df['거래금액'].dtypes != object):
        print("Don't need filtering")
        df = df.astype('object')
    else:
        print("Need filtering")
        filter = df['거래금액'] != '거래금액'
        df = df[filter]

    filteringdf = df.sort_index()

    # 인덱스 번호 재시작
    filteringdf.reset_index(drop=True, inplace=True)

    return filteringdf

"""
*******************************************************************************
* do_ReadData(_item, _year, _startMonth, _endMonth)       

* @brief                                        Read Target item

* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] int           _year               Target item year
* @param [in] int           _startMonth         Target item start month
* @param [in] int           _endMonth           Target item end month

* @return     DataFrame                         Return target year merged data             
*******************************************************************************
"""
def do_ReadData(_item, _year, _startMonth, _endMonth):
    S3Client = Init_S3Client()

    ConcatData = pd.DataFrame()

    for month in range(_startMonth, _endMonth+1):
    
        tempData = pd.DataFrame()

        if month <= 0 | month > 13:
            print("Out of range")

        if month < 10:
            tempData = Read_Trade(_s3Obj= S3Client, 
                                    _item= _item, 
                                    _year= str(_year),
                                    _month="0"+str(month))
        else:
            tempData = Read_Trade(_s3Obj= S3Client, 
                                    _item= _item, 
                                    _year= str(_year), 
                                    _month=str(month))

        ConcatData = pd.concat([ConcatData, tempData], axis=0, ignore_index=True)

    return ConcatData
# %%
MultiHouse15 = do_ReadData(_item="multi_housing", _year=2015, _startMonth=1, _endMonth=12)
print(MultiHouse15)

# CHECK : 04누락
# MultiHouse16 = do_ReadData(_item="multi_housing", _year=2016, _startMonth=1, _endMonth=12)
# print(MultiHouse16)

# CHECK : 12누락
# MultiHouse17 = do_ReadData(_item="multi_housing", _year=2017, _startMonth=1, _endMonth=12)
# print(MultiHouse17)

# CHECK : 03누락
# MultiHouse18 = do_ReadData(_item="multi_housing", _year=2018, _startMonth=1, _endMonth=12)
# print(MultiHouse18)

# CHECK : 06누락
# MultiHouse19 = do_ReadData(_item="multi_housing", _year=2019, _startMonth=1, _endMonth=12)
# print(MultiHouse19)

# CHECK : 01누락
# MultiHouse20 = do_ReadData(_item="multi_housing", _year=2020, _startMonth=1, _endMonth=7)
# print(MultiHouse20)