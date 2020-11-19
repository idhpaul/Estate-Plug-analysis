#%%
# Init S3 Env
# conda install pylint

import datetime
from re import A
import sys
import pandas as pd
import numpy as np

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

    hasData = False

    s3FileName = _item + "_trade_" + _year + _month + ".csv"
    print(s3FileName)

    for key in _s3Obj.list_objects(Bucket='store-estate')['Contents']:
        if s3FileName == key['Key']:
            print("Item detected")
            hasData = True
            break;


    if hasData == False:
        print("No Item")
        return pd.DataFrame()

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
* Read_Contract(_s3Obj, _item, _year, _month) 

* @brief                                        Read Target item

* @param [in] botostubs.S3  _s3Obj              S3 Client / Init_S3Client() 로드 필수
* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] string        _year               Target item year
* @param [in] string        _month              Target item month

* @return     DataFrame                         Return target month for the year DataFrame  
*******************************************************************************
"""
def Read_Contract(_s3Obj, _item, _year, _month):

    hasData = False

    s3FileName = _item + "_contract_" + _year + _month + ".csv"
    print(s3FileName)

    for key in _s3Obj.list_objects(Bucket='store-estate')['Contents']:
        if s3FileName == key['Key']:
            print("Item detected")
            hasData = True
            break;


    if hasData == False:
        print("No Item")
        return pd.DataFrame()

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
def do_ReadContractData(_item, _year, _startMonth, _endMonth):
    S3Client = Init_S3Client()

    ConcatData = pd.DataFrame()

    for month in range(_startMonth, _endMonth+1):
    
        tempData = pd.DataFrame()

        if month <= 0 | month > 13:
            print("Out of range")

        if month < 10:
            tempData = Read_Contract(_s3Obj= S3Client, _item= _item, _year= str(_year),_month="0"+str(month))

        else:
            tempData = Read_Contract(_s3Obj= S3Client, _item= _item, _year= str(_year), _month=str(month))

        # No Data in S3
        if tempData.empty:
            print("No Data in S3")
            continue

        ConcatData = pd.concat([ConcatData, tempData], axis=0, ignore_index=True)

    return ConcatData

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
def do_ReadTradeData(_item, _year, _startMonth, _endMonth):
    S3Client = Init_S3Client()

    ConcatData = pd.DataFrame()

    for month in range(_startMonth, _endMonth+1):
    
        tempData = pd.DataFrame()

        if month <= 0 | month > 13:
            print("Out of range")

        if month < 10:
            tempData = Read_Trade(_s3Obj= S3Client, _item= _item, _year= str(_year),_month="0"+str(month))

        else:
            tempData = Read_Trade(_s3Obj= S3Client, _item= _item, _year= str(_year), _month=str(month))

        # No Data in S3
        if tempData.empty:
            print("No Data in S3")
            continue

        ConcatData = pd.concat([ConcatData, tempData], axis=0, ignore_index=True)

    return ConcatData


"""
*******************************************************************************
* calc_trade_yearly_increase(_item, _startYear, _endYear)       

* @brief                                        연도 별 거래 증감량
* @note
*           Graph Type                          선형 그래프
*           MongoDB Collection name :           $(item)_trade_yearly_increase
*           MongoDB Scehma                      
*                                               {
                                                    year: '2020',              <- String
                                                    value: 1                   <- Int
                                                }

* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] int           _startYear          Target item start year
* @param [in] int           _endYear            Target item end year
         
*******************************************************************************
"""
def calc_trade_yearly_increase(_item, _startYear, _endYear):

    flag_first = True
    represent_value = 0

    target_collection_name = _item + "_trade_yearly_increase"

    for i in range(_startYear, _endYear+1):
        tempdf = do_ReadTradeData(_item="apart", _year=i, _startMonth=1, _endMonth=12)

        if flag_first == True:
            represent_value = tempdf['월'].count()
            flag_first = False

        target_value = represent_value - tempdf['월'].count()

        print(i)
        print(int(target_value))

        myDB = Connect_MongoDB()
        myCol = myDB[target_collection_name]
        x = myCol.insert_one({"year":i, "value":int(target_value)})
        print(x.inserted_id)

"""
*******************************************************************************
* calc_trade_monthly_increase(_data, _item, _year, _startMonth, _endMonth)       

* @brief                                        월 별 거래 증감량
* @note
*           Graph Type                          선형 그래프
*           MongoDB Collection name :           $(item)_trade_monthly_increase
*           MongoDB Scehma                      
                                                {
                                                    year_month: '202010',      <- String
                                                    value: 1                   <- Int
                                                }

* @param [in] DataFrame     _data               Target Data
* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] int           _year               Target item year
* @param [in] int           _startMonth         Target item end month
* @param [in] int           _endMonth           Target item end month
       
*******************************************************************************
"""
def calc_trade_monthly_increase(_data, _item, _year, _startMonth, _endMonth):

    flag_first = True
    represent_value = 0

    target_collection_name = _item + "_trade_monthly_increase"

    _data['월'] = pd.to_numeric(_data['월'])
    _data['거래금액'] = pd.to_numeric(_data['거래금액'])
    _data.dtypes

    for i in range(_startMonth, _endMonth+1):
        target_year = str(_year)
        if i < 10:
            target_month = "0"+str(i)
        else:
            target_month = str(i)

        if flag_first == True:
            represent_value = _data['월'][_data['월']==i].count()
            flag_first = False

        target_value = represent_value - _data['월'][_data['월']==i].count()

        print(target_year, target_month)
        print(int(target_value))

        myDB = Connect_MongoDB()
        myCol = myDB[target_collection_name]
        x = myCol.insert_one({"year_month":target_year + target_month, "value":int(target_value)})
        print(x.inserted_id)


"""
*******************************************************************************
* calc_trade_monthly_price(_data, _item, _year, _startMonth, _endMonth)       

* @brief                                        월 별 거래금액량
* @note
*           Graph Type                          막대 그래프
*           MongoDB Collection name :           $(item)_trade_monthly_price
*           MongoDB Scehma                      
                                                {
                                                    year_month: '202010',      <- String
                                                    price: 250102              <- Int
                                                }

* @param [in] DataFrame     _data               Target Data
* @param [in] string        _item               Target item type(Available value(string type) = apart, detached, land, multi_houing, officetel)
* @param [in] int           _year               Target item year
* @param [in] int           _startMonth         Target item end month
* @param [in] int           _endMonth           Target item end month
* @return     DataFrame                         Return target year mom data 

*******************************************************************************
"""
def calc_trade_monthly_price(_data, _item, _year, _startMonth, _endMonth):
    
    target_collection_name = _item + "_trade_monthly_price"

    _data['월'] = pd.to_numeric(_data['월'])
    _data['거래금액'] = pd.to_numeric(_data['거래금액'])
    _data.dtypes

    for i in range(_startMonth, _endMonth+1):

        target_year = str(_year)
        if i < 10:
            target_month = "0"+str(i)
        else:
            target_month = str(i)
        target_price = _data['거래금액'][_data['월']==i].sum()
        print(target_year, target_month)
        print(int(target_price))

        myDB = Connect_MongoDB()
        myCol = myDB[target_collection_name]
        x = myCol.insert_one({"year_month":target_year + target_month, "price":int(target_price)})
        print(x.inserted_id)
def find_area_name(_areaCode):
    
    area_name = ""

    if(_areaCode == 11110):
        area_name = "종로구"
    elif(_areaCode == 11140):
        area_name = "중구"
    elif(_areaCode == 11170):
        area_name = "용산구"
    elif(_areaCode == 11200):
        area_name = "성동구"
    elif(_areaCode == 11215):
        area_name = "광진구"
    elif(_areaCode == 11230):
        area_name = "동대문구"
    elif(_areaCode == 11260):
        area_name = "중랑구"
    elif(_areaCode == 11290):
        area_name = "성북구"
    elif(_areaCode == 11305):
        area_name = "강북구"
    elif(_areaCode == 11320):
        area_name = "도봉구"
    elif(_areaCode == 11350):
        area_name = "노원구"
    elif(_areaCode == 11380):
        area_name = "은평구"
    elif(_areaCode == 11410):
        area_name = "서대문구"
    elif(_areaCode == 11440):
        area_name = "마포구"
    elif(_areaCode == 11470):
        area_name = "양천구"
    elif(_areaCode == 11500):
        area_name = "강서구"
    elif(_areaCode == 11530):
        area_name = "구로구"
    elif(_areaCode == 11545):
        area_name = "금천구"
    elif(_areaCode == 11560):
        area_name = "영등포구"
    elif(_areaCode == 11590):
        area_name = "동작구"
    elif(_areaCode == 11620):
        area_name = "관악구"
    elif(_areaCode == 11650):
        area_name = "서초구"
    elif(_areaCode == 11680):
        area_name = "강남구"
    elif(_areaCode == 11710):
        area_name = "송파구"
    elif(_areaCode == 11740):
        area_name = "강동구"

    return area_name

def calc_trade_yearly_percentage(_item, _year):

    target_collection_name = _item + "_trade_yearly_percentage"

    read_target_trade_data = do_ReadTradeData(_item=_item, _year=_year, _startMonth=1, _endMonth=12)
    temp = read_target_trade_data

    temp['지역코드'] = pd.to_numeric(temp['지역코드'])
    seoul_temp = temp[temp.columns][temp['지역코드']<=20000]

    temp = seoul_temp.groupby('지역코드').count()

    new_index = temp.index
    new_column = temp['거래금액']
    new_column.name = '지역별 거래 비율'

    new_pd = new_column.to_frame()
    new_pd

    hole_count=seoul_temp['거래금액'].count()
    hole_count

    new_pd = new_pd * 100 / hole_count
    new_pd

    sort_data = new_pd.sort_values('지역별 거래 비율',ascending=False)

    for i in range(1, 25+1):
        print(i)
        target_year = str(_year)
        target_name = find_area_name(sort_data.iloc[i-1].name)
        target_percent = np.round_(sort_data.iloc[i-1].values.astype(float),2)
        target_percent = target_percent[0]
        print(target_year, target_name)
        print(target_percent)

        myDB = Connect_MongoDB()
        myCol = myDB[target_collection_name]
        x = myCol.insert_one({"year":target_year, "area_cd":target_name, "percent":target_percent})
        print(x.inserted_id)

 
# %%
# 연도 별 데이터 추합 (userspace)
Land15 = do_ReadTradeData(_item="land", _year=2015, _startMonth=1, _endMonth=12)
print(Land15)

Land16 = do_ReadTradeData(_item="land", _year=2016, _startMonth=1, _endMonth=12)
print(Land16)

Land17 = do_ReadTradeData(_item="land", _year=2017, _startMonth=1,  _endMonth=12)
print(Land17)

Land18 = do_ReadTradeData(_item="land", _year=2018,  _startMonth=1, _endMonth=12)
print(Land18)

Land19 = do_ReadTradeData(_item="land", _year=2019,  _startMonth=1, _endMonth=12)
print(Land19)

Land20 = do_ReadTradeData(_item="land", _year=2020, _startMonth=1, _endMonth=12)
print(Land20)


# %%
# 주택 월 별 거래금액(막대그래프) (userspace)
calc_trade_monthly_price(_data= Land15, _item= "land",_year=2015, _startMonth=1, _endMonth=12)
calc_trade_monthly_price(_data= Land16, _item= "land", _year=2016, _startMonth=1, _endMonth=12)
calc_trade_monthly_price(_data= Land17, _item= "land",_year=2017, _startMonth=1, _endMonth=12)
calc_trade_monthly_price(_data= Land18, _item= "land", _year=2018, _startMonth=1, _endMonth=12)
calc_trade_monthly_price(_data= Land19, _item= "land",_year=2019, _startMonth=1, _endMonth=12)
calc_trade_monthly_price(_data= Land20, _item= "land",_year=2020, _startMonth=1, _endMonth=12)

# %%
# 주택 월 별 거래 증감률(선형 그래프) (userspace)

# TODO : Need '월' column filtering (it has like '개발제한구역', '자연녹지지역')

calc_trade_monthly_increase(_data= Land15, _item= "land",_year=2015, _startMonth=1, _endMonth=12)
calc_trade_monthly_increase(_data= Land16, _item= "land",_year=2016, _startMonth=1, _endMonth=12)
calc_trade_monthly_increase(_data= Land17, _item= "land",_year=2017, _startMonth=1, _endMonth=12)
calc_trade_monthly_increase(_data= Land18, _item= "land",_year=2018, _startMonth=1, _endMonth=12)
calc_trade_monthly_increase(_data= Land19, _item= "land",_year=2019, _startMonth=1, _endMonth=12)
calc_trade_monthly_increase(_data= Land20, _item= "land",_year=2020, _startMonth=1, _endMonth=12)
# %%
# 주택 연도 별 거래 증감률(선형 그래프) (userspace)
calc_trade_yearly_increase(_item= "land", _startYear=2015, _endYear=2020)

#%%
# 아파트 연도 별 거래 비중(원 그래프) (userspace)
calc_trade_yearly_percentage(_item= "land", _year=2015)
calc_trade_yearly_percentage(_item= "land", _year=2016)
calc_trade_yearly_percentage(_item= "land", _year=2017)
calc_trade_yearly_percentage(_item= "land", _year=2018)
calc_trade_yearly_percentage(_item= "land", _year=2019)
calc_trade_yearly_percentage(_item= "land", _year=2020)

