import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql import SparkSession
from azure.cosmosdb.table.tableservice import TableService
import pandas as pd
import datetime as dt
import json
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from cryptography.fernet import Fernet as F
import hashlib
# from Crypto.Cipher import AES
import base64
import os
# import pyspark.pandas as ps
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.acs_exception.exceptions import ClientException
from aliyunsdkcore.acs_exception.exceptions import ServerException
from aliyunsdkcore.auth.credentials import StsTokenCredential
from aliyunsdkcore.request import CommonRequest
from alibabacloud_credentials.client import Client
from alibabacloud_credentials.models import Config
from aliyunsdkkms.request.v20160120 import GetSecretValueRequest

class DataUploader:
    def __init__(self, secrets_manager, loadmode="full", deltaloaddays=3, encryption=True, batchload=False, batchsize=0):
        self.secrets_manager = secrets_manager
        self.loadmode = loadmode
        self.deltaloaddays = deltaloaddays
        self.encryption = encryption
        self.batchload = batchload
        self.batchsize = batchsize
        
        self.landing_Access_token = secrets_manager.get("udp-infra", "udpcnm-app-blob-conn")
        self.access_key=secrets_manager.get("udp-infra", "sftp-ak")
        self.secret_key=secrets_manager.get("udp-infra", "sftp-sk")
        self.role_arn=secrets_manager.get("udp-infra", "sftp-role-arn")
        self.secret_name=secrets_manager.get("udp-infra", "sftp-secret-name")

        self.container_name = 'sftp'
        self.Current_Time = dt.datetime.now() + dt.timedelta(hours=8)

    def get_sts_token(self):
        cl = AcsClient(self.access_key, self.secret_key, "cn-shanghai")
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain('sts.aliyuncs.com')
        request.set_method('POST')
        request.set_protocol_type('https')  # https | http
        request.set_version('2015-04-01')
        request.set_action_name('AssumeRole')
        request.add_query_param("RoleArn", self.role_arn)
        request.add_query_param("RoleSessionName", "alink")
        response = cl.do_action_with_exception(request)
        response=json.loads(str(response, encoding='utf-8'))
        Credentials_AccessKeyId=response["Credentials"]['AccessKeyId']
        Credentials_AccessKeySecret=response["Credentials"]["AccessKeySecret"]
        Credentials_SecurityToken=response["Credentials"]['SecurityToken']
        return Credentials_AccessKeyId,Credentials_AccessKeySecret,Credentials_SecurityToken

    def get_secret_value(self, Credentials_AccessKeyId, Credentials_AccessKeySecret, Credentials_SecurityToken):
        sts_token_credential = StsTokenCredential(Credentials_AccessKeyId, Credentials_AccessKeySecret, Credentials_SecurityToken)
        client = AcsClient(region_id='cn-shanghai', credential=sts_token_credential)
        request = GetSecretValueRequest.GetSecretValueRequest()
        request.set_SecretName(self.secret_name)
        request.set_FetchExtendedConfig(False)
        response = client.do_action_with_exception(request)
        content = response.decode('utf-8')
        content=json.loads(str(content))
        key=content['SecretData']
        return key

    def base64_encode_key(self):
        Credentials_AccessKeyId,Credentials_AccessKeySecret,Credentials_SecurityToken=self.get_sts_token()
        key=self.get_secret_value(Credentials_AccessKeyId,Credentials_AccessKeySecret,Credentials_SecurityToken)
        original_key = key if isinstance(key, bytes) else key.encode()
        ##32 byte key required in fernect AES 265 algorithm
        extended_key = hashlib.sha256(original_key).digest()
        ### Fernet key must be 32 url-safe base64-encoded bytes.
        encoded_bytes = base64.urlsafe_b64encode(extended_key)
        return encoded_bytes
    

    def encrypt_data(self, data):
        encoded_key=self.base64_encode_key()
        fernet=F(encoded_key)
        by_data=data.encode('utf-8')
        ## AES 256 encryption: 
        encrypted_data=fernet.encrypt(by_data)
        return encrypted_data


    def extract_string(self, text, r_split, l_split):
       # 使用 rsplit() 方法将字符串从右侧开始以 "-" 分隔成两个部分
        last_part = text.rsplit(r_split, 1)[-1]
        # 使用 split() 方法将截取得到的字符串从左侧开始以 "." 分隔成两个部分
        result = last_part.split(l_split, 1)[0]
        return result

    def upload_data_cdc(self, querystr, deltacolumn, filename):
        time_str = self.Current_Time.strftime("%Y%m%d%H%M%S%f")[:-5]

        blob_service_client = BlobServiceClient.from_connection_string(self.landing_Access_token)
        container_client = blob_service_client.get_container_client(self.container_name)
        ##get all blob list
        prefix_str='Outbound/'+filename
        blob_list = container_client.list_blobs(name_starts_with=prefix_str)
        file_list=[]
        delete_list=[]
        for i in blob_list:
            blob_name_i=i.name
            file_list.append(blob_name_i)
            if 'enc-' in blob_name_i:
                time_blob_str=self.extract_string(blob_name_i,'enc-','.')
            else:
                time_blob_str=self.extract_string(blob_name_i,r'full-|delta-','.')
            if load_mode=='delta':
                if int(time_blob_str[:8])<int(time_str[:8]):
                    delete_list.append(i.name)
            else:
                delete_list.append(i.name)
        keep_list=list(set(file_list)-set(delete_list))

        ##delete blob
        for i in delete_list:
            blob_client = container_client.get_blob_client(i)
            blob_client.delete_blob()
            print(f'delete blob {i}')
        ### upload data
        #### save memery
        print('convert data into json')
        # json_rdd = df.toJSON()
        ### combine sql string
        if self.loadmode=="delta":
            start_time=self.Current_Time+dt.timedelta(days=-1*self.deltaloaddays) 
            if 'where' in querystr:
                sql_string=querystr+f"and date_format({deltacolumn}+interval 8 hours ,'yyyy-MM-dd')>=date_format('{start_time}', 'yyyy-MM-dd')"
            elif 'where' not in querystr:
                sql_string=querystr+f"where date_format({deltacolumn}+interval 8 hours ,'yyyy-MM-dd')>=date_format('{start_time}', 'yyyy-MM-dd')"
        else:
            sql_string=querystr
        if self.batchload==True:
            k=1
            while True:
                sql_query=sql_string+f" ORDER BY (select NULL) LIMIT {self.batchsize} OFFSET "+str((k-1)*self.batchsize+0)
                df=spark.sql(f"""{sql_query}""")
                pdf = df.toPandas()
                json_df=pdf.to_json(orient='records',force_ascii=False,date_format='iso')
                print('data encrypt')
                if self.encryption==True:
                    output=self.encrypt_data(json_df)
                    encryption_suffix='enc-'
                else:
                    output=json_df
                    encryption_suffix=''
                    
                if self.loadmode=='delta':
                    delta_times=[int(self.extract_string(i,'load','-')) for i in keep_list if 'delta' in i]
                    max_times=max(delta_times) if delta_times else 0
                    blob_name='Outbound/'+filename+'-delta-'+encryption_suffix+f'{time_str}'+f'-load{max_times+1}-batch{k}.json'
                else:
                    blob_name='Outbound/'+filename+'-full-'+encryption_suffix+f'{time_str}-batch{k}.json'
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(output, overwrite=False)
                k+=1
                print(f'uploaded {blob_name} to blob storage loction {self.container_name} with count: {df.count()}')
                if pdf.shape[0]<self.batchsize:
                    break
        else:
            sql_query=sql_string
            df=spark.sql(f"""{sql_query}""")
            pdf = df.toPandas()
            json_df=pdf.to_json(orient='records',force_ascii=False,date_format='iso')
    
            print('data encrypt')
            if self.encryption==True:
                output=self.encrypt_data(json_df)
                encryption_suffix='enc-'
            else:
                output=json_df
                encryption_suffix=''
                
            if self.loadmode=='delta':
                delta_times=[int(self.extract_string(i,'_','.')) for i in keep_list if 'delta' in i]
                max_times=max(delta_times) if delta_times else 0
                blob_name='Outbound/'+filename+'-delta-'+encryption_suffix+f'{time_str}'+f'-load{max_times+1}.json'
            else:
                blob_name='Outbound/'+filename+'-full-'+encryption_suffix+f'{time_str}.json'
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(output, overwrite=False)
            print(f'uploaded {blob_name} to blob storage loction {self.container_name} with count: {df.count()}')


class DatabricksSecretsManager:
    def get(self, scope, key):
        return dbutils.secrets.get(scope, key)
