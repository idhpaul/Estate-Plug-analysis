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
# %%
S3Client = Init_S3Client()
trade_2020_01 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="01")
trade_2020_02 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="02")
trade_2020_03 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="03")
trade_2020_04 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="04")
trade_2020_05 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="05")
trade_2020_06 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="06")
trade_2020_07 = Read_Trade(_s3Obj= S3Client, _item= "apart", _year= "2020", _month="07")

# %%
test = trade_2020_07.sort_values(by='거래금액',ascending=False)
filter = test['거래금액'] != '거래금액'
filtering = test[filter]
filtering

filtering = filtering.sort_index()
filtering

filtering.reset_index(drop=True, inplace=True)
filtering

# %%
