from email import message
from itertools import count
from wsgiref import headers
import pyodbc
import json
import requests
import traceback
import time
from azure.storage.blob import ResourceTypes, AccountSasPermissions, generate_account_sas
from datetime import datetime, timedelta
from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
import os
import sys


#Azure secrets
credential_chain = ChainedTokenCredential(ManagedIdentityCredential(), AzureCliCredential())
client = SecretClient(vault_url='https://dkma-prod-data-extract.vault.azure.net/', credential=credential_chain)
#Database connection
server = client.get_secret('dkma-db-server').value
database = client.get_secret('dkma-database').value
#pwd = client.get_secret('dkma-db-psd').value
uid = client.get_secret('dkma-db-usr').value
pwd= 'DKMA_2024!!!'
#uid = 'hegdes'
db_schema = client.get_secret('dkma-stage-schema').value
DRIVER= '{ODBC Driver 17 for SQL Server}' #Name of driver for connection
connection_string = 'DRIVER='+DRIVER+';SERVER='+server+';DATABASE='+database+';UID='+uid+';PWD='+pwd+''

#SAS token creation
acc_key = client.get_secret('dkma-storage-acckey').value
blob_storage = client.get_secret('dkma-blob-storage').value

sas_token = generate_account_sas(
blob_storage,
account_key=acc_key,
resource_types=ResourceTypes(object=True),
permission=AccountSasPermissions(read=True),
expiry=datetime.utcnow() + timedelta(hours=24)
)
print("token Generated")
print(sas_token)

x_api_key = client.get_secret('x-api-key').value
container = client.get_secret('container').value
put_url = client.get_secret('halo-dkma-doc-put-api').value
# put_url ='https://sl1q9i7ypc.execute-api.eu-west-1.amazonaws.com/prod/migration/putapi'

try:
    batch_number = int(input("Enter the Batch number = "))
except:
    print('Please execute the code again and enter a valid Batch number')
    sys.exit(1)

while(True):
    connection = pyodbc.connect(connection_string,autocommit=True)
    c1 = connection.cursor()
    c2 = connection.cursor()
    print('connection created')
    data_list = ("""SELECT count(BATCH_IMPORT_STATUS) FROM {}.dm_sentinel_case_haloimport_bkp where DOC_UPLOADED = 0 and batch_import_status != 0 """).format(db_schema)
    sql = c1.execute(data_list).fetchone()
    print("Total batches for document upload:"+str(sql[0]))
    if sql[0] == 0:
        c1.close()
        c2.close()
        connection.close()
        print("There is now batch for document uploading, Let's wait for few minutes")
        time.sleep(100)
        continue
    main_sql = ("""SELECT HALO_RESPONSE_JSON,HALO_COMM_JSON,BATCH_NO,GENERIC_CASE_ID FROM {}.dm_sentinel_case_haloimport_bkp WHERE DOC_UPLOADED = 0 ORDER BY BATCH_NO""").format(db_schema)
    ##.format(db_schema) 
    ##""").format(db_schema)
    ##DOC_UPLOADED = 0 ORDER BY BATCH_NO""").format(db_schema)
    fail_sql = ("""Insert into {}.[DM_SENTINEL_DOC_DETAILS] (ADR_NUMBER,SUBMISSION_NUMBER,BATCH_NO,DOCUMENTUM_ID, HALO_DOC_ID,DOC_STATUS,ERROR_MESSAGE) values (?,?,?,?,?,?,?)""").format(db_schema)

    pass_sql = ("""Insert into {}.[DM_SENTINEL_DOC_DETAILS] (ADR_NUMBER,SUBMISSION_NUMBER,BATCH_NO,DOCUMENTUM_ID,HALO_DOC_ID,DOC_STATUS,ERROR_MESSAGE) values (?,?,?,?,?,?,?)""").format(db_schema)    
    batch = c1.execute(main_sql)
    
    for row in batch.fetchall():
        counter = 0
        error_message = ''
        batch_id = str(row.BATCH_NO)
        generic_id = row.GENERIC_CASE_ID
        print("Running for batch:"+batch_id)
        try:
            batch_details = json.loads(row[0])['batch_details']
            for i in batch_details:
                try:
                    revision = i['adr_details']['revision_details']
                    adr_number = i['adr_details']['adr_number']
                    master_id = i['adr_details']['master_id']

                except:
                    error_message = error_message +'Batch: {}, adr_number: {}'.format(batch_id,adr_number)+' Error Message: ' +traceback.format_exc() + '\n'
                    counter+=1
                for j in revision:
                    # master_id = j['master_id']
                    revision = j['revision']
                    if 'doc_json' in j.keys():
                        try:
                            if j['doc_json'] == None:
                                pass
                                print("no document")
                            else:
                                doc_json = json.loads(j['doc_json'])['documents']
                                for k in doc_json:
                                    try:
                                        doc_id=k['halo_doc_id']
                                        docname = k['docname']
                                        azurefilepath = k['azurefilepath']
                                        appfilename = k['appfilename']
                                        documentum_doc_id = k['documentum_doc_id']  #Need to add documentum id in tabl
                                        if documentum_doc_id != None: 
                                            try:
                                                azure_url ='https://'+blob_storage+'.blob.core.windows.net/'+container+azurefilepath+'/'+docname+'?'+sas_token
                                                doc_cont = requests.get(azure_url, stream=True)     # Document fetching from blob storage
                                                if doc_cont.status_code > 299:
                                                    counter+= 1
                                                    c2.execute(fail_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,3,doc_cont.content)
                                                else:
                                                    try:
                                                        key = 'General'+'/'+str(master_id)+'/'+'REV' + str(revision)
                                                        headers={'Content-Type': 'application/octet-stream','path':key,'filename':docname,
                                                                'api_key':'813088e9-13f6-4016-a42e-756ccdb7e446', 'x-api-key': x_api_key,
                                                                'request_id':'','doc_store_id':str(doc_id)}

                                                        doc_upload = requests.post(put_url,doc_cont.content,headers = headers)

                                                        if doc_upload.status_code == 413:
                                                            c2.execute(fail_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,3,'File size is too long it should be 10MB')
                                                            pass
                                                        
                                                        elif doc_upload.status_code > 299:
                                                            counter+= 1
                                                            c2.execute(fail_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,3,(doc_upload.content)[0:3000])
                                                            print('Fail')
                                                        else:    
                                                            c2.execute(pass_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,2,'null')
                                                            print('Pass')
                                                            
                                                    except:
                                                        counter+=1
                                                        c2.execute(fail_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,3,traceback.format_exc())
                                                        error_message = error_message +'Batch: {}, adr_number: {},Document Unique Id:{}, HALO Doc ID:{}'.format(batch_id,adr_number,documentum_doc_id,doc_id)+' Error Message: ' +traceback.format_exc() + '\n'
                                                        print("Fail")
                                            except:
                                                counter+=1
                                                error_message = error_message +'Batch: {}, adr_number: {},Document Unique Id:{}, HALO Doc ID:{}'.format(batch_id,adr_number,documentum_doc_id,doc_id)+' Error Message: ' +traceback.format_exc() + '\n'
                                                c2.execute(fail_sql,adr_number,revision,batch_id,documentum_doc_id,doc_id,3,traceback.format_exc())

                                    except:
                                        counter+=1
                                        error_message = error_message +'Batch: {}, adr_number: {}'.format(batch_id,adr_number)+' Error Message: ' +traceback.format_exc() + '\n'
                        except:
                            counter+=1
                            error_message = error_message +'Batch: {}, adr_number: {}'.format(batch_id,adr_number)+' Error Message: ' +traceback.format_exc() + '\n'   
        except:
            error_message = error_message +'Batch: {}'.format(batch_id)+' Error Message: ' +traceback.format_exc() + '\n'
            counter+=1

        if counter == 0:
            batch_pass_sql = ("""UPDATE {}.DM_SENTINEL_CASE_HALOIMPORT_bkp SET DOC_UPLOADED = 2 WHERE GENERIC_CASE_ID=?""").format(db_schema)
            c2.execute(batch_pass_sql,generic_id)
        else:
            batch_fail_sql = ("""UPDATE {}.DM_SENTINEL_CASE_HALOIMPORT_bkp SET DOC_UPLOADED = 3,error_message = ?  WHERE GENERIC_CASE_ID=?""").format(db_schema)
            c2.execute(batch_fail_sql,error_message[0:3995],generic_id)
    c1.close()
    c2.close()
    connection.close()