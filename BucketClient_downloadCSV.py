#%%
# conda update conda
# pip install boto3 botostubs

#package
# Anaconda Extension Pack
# GitLens
# MagicPython
# MongoDB
# Pylance
# Python
# select highlihg in minimap
# Visual studio IntelliCode
# Visual Studio Keymap
# vscode-icons

# boto3 intellicode
# ref https://trevorsullivan.net/2019/06/11/intellisense-microsoft-vscode-aws-boto3-python/

import sys
import pandas as pd 
import boto3
import botostubs

if sys.version_info[0] < 3: 
    from StringIO import StringIO # Python 2.x
else:
    from io import StringIO # Python 3.x

AWS_ACCESS_KEY_ID ="AKIAT4IAQPVVMMBS2FNX"
AWS_SECRET_ACCESS_KEY = "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"
AWS_DEFAULT_REGION = "ap-northeast-2"
client = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_DEFAULT_REGION
                      ) # type: botostubs.S3


#%%
# Retrieve the list of existing buckets
response = client.list_buckets()
print(response)

# Output the bucket names
print('Existing buckets:')
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')


#%%
# S3 버킷 파일 리스트 출력
for key in client.list_objects(Bucket='store-estate')['Contents']:
    print(key['Key'])

#%%

# S3 버킷 파일 다운로드
client.download_file('store-estate', 'detached_contract_201602.csv', 'testcsv.csv')

# %%

# S3 버킷 파일 모두 다운로드 (이진 형태로 다운로드 됨)

for key in client.list_objects(Bucket='store-estate')['Contents']:
    print(key['Key'])
    print("Downlad File...")

    #client.download_file('store-estate', key['Key'], key['Key'])

print("Done")
# %%

# S3 파일 CSV파일 DataFrame으로 읽기
# ref : https://stackoverflow.com/questions/30818341/how-to-read-a-csv-file-from-an-s3-bucket-using-pandas-in-python/46323684#46323684
csv_obj = client.get_object(Bucket='store-estate', Key='officetel_trade_201602.csv')
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))
print(df)

# %%

# S3 버킷 파일별 CSV 인코딩 저장
for key in client.list_objects(Bucket='store-estate')['Contents']:
    print(key['Key'])
    print("Downlad File...")

    filename = key['Key']

    csv_obj = client.get_object(Bucket='store-estate', Key=filename)
    body = csv_obj['Body']
    csv_string = body.read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_string))

    df.to_csv(filename, index=False, encoding='cp949')

print("Download Done!")
# %%
# TEST Dataframe(apart_contract_201501.csv)
csv_obj = client.get_object(Bucket='store-estate', Key="apart_contract_201501.csv")
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

# Drop Unnecessary Columns 
df = df.drop(df.columns[0], axis='columns')

# Filter Unnecessary Rows
is_Unnecessary = df['건축년도'] == '건축년도'

df1 = df[~is_Unnecessary]
print(df1)
# %%
# TEST Dataframe(apart_contract_201501.csv)
csv_obj = client.get_object(Bucket='store-estate', Key="apart_contract_201502.csv")
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

# Drop Unnecessary Columns 
df = df.drop(df.columns[0], axis='columns')

# Filter Unnecessary Rows
is_Unnecessary = df['건축년도'] == '건축년도'

df2 = df[~is_Unnecessary]

# %%
# 아파트 거래량 테스트(apart_trade_201501.csv)

csv_obj = client.get_object(Bucket='store-estate', Key="apart_trade_201501.csv")
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

# Drop Unnecessary Columns 
df = df.drop(df.columns[0], axis='columns')

# grouped_code = df.groupby(['지역코드'])
# for key, group in grouped_code:
#     print("* key", key)
#     print("* count", len(group))
#     print(group.head())
#     print('\n')

df_grouped1 = df.groupby(['지역코드']).size().reset_index(name='counts')
print(df_grouped1)
print(df_grouped1.dtypes)

df_grouped1 = df_grouped1.astype({'지역코드':int})
print(df_grouped1)

# %%
# 아파트 거래량 테스트(apart_trade_201502.csv)

csv_obj = client.get_object(Bucket='store-estate', Key="apart_trade_201502.csv")
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

df = pd.read_csv(StringIO(csv_string))

# Drop Unnecessary Columns 
df = df.drop(df.columns[0], axis='columns')
# Filter Unnecessary Rows
is_Unnecessary = df['지역코드'] == '지역코드'

df = df[~is_Unnecessary]

# grouped_code = df.groupby(['지역코드'])
# for key, group in grouped_code:
#     print("* key", key)
#     print("* count", len(group))
#     print(group.head())
#     print('\n')

df_grouped2 = df.groupby(['지역코드']).size().reset_index(name='counts')
print(df_grouped2)
print(df_grouped2.dtypes)

df_grouped2 = df_grouped2.astype({'지역코드':int})
print(df_grouped2)

# %%
# TODO : 지역코드별 월별 증감률 Dataframe화
df_grouped1 - df_grouped2

# %%
