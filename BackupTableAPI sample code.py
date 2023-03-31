import base64, requests, sys, json, pandas as pd, datetime as dt

#region
LAB_Env = 'usw2.pure.cloud'
LAB_Instance = 'LAB'
LAB_ID = 'sensitive data'
LAB_Secret = 'sensitive data'
#endregion


#############################################################
######                 TOKEN API MODULE                ######
#############################################################

def get_token():
    # Base64 encode the client ID and client secret
    authorization = base64.b64encode(bytes(CLIENT_ID + ":" + CLIENT_SECRET, "ISO-8859-1")).decode("ascii")

    # Prepare for POST /oauth/token request
    request_headers = {
        "Authorization": f"Basic {authorization}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    request_body = {
        "grant_type": "client_credentials"
    }

    # Get token
    response = requests.post(f"https://login.{instance}/oauth/token", data=request_body, headers=request_headers)

    # Check response
    if response.status_code == 200:
        print("Got token")
    else:
        print(f"Failure: { str(response.status_code) } - { response.reason }")
        sys.exit(response.status_code)

    # Get JSON response body
    response_json = response.json()
    return(response_json['access_token'])




#############################################################
######                 LIST API MODULE                 ######
#############################################################


def get_table_list_api(gtl_n):
    # Prepare for GET 
    request_headers = {
        #'Authorization': f"{ response_json['token_type'] } { response_json['access_token']}",
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }
    gtl_size = 500
    gtl_param = f'?pageNumber={gtl_n}&pageSize={gtl_size}'
    # Get API data
    gtl_api_response = requests.get(f"https://api.{instance}/api/v2/flows/datatables/divisionviews{gtl_param}", headers=request_headers)
    # Check response
    if gtl_api_response.status_code == 200:
        return gtl_api_response.content

    else:
        print(f"Failure: {str(gtl_api_response.status_code)} - {gtl_api_response.reason}")
        return gtl_api_response.content 


def get_table_list():
    list_n=1
    list_pages = 1
    list_final_frame = []
    while list_n < list_pages+1:
        if list_n == 1: #first iteration defines number of pages and takes first portion of data
            list_data = json.loads(get_table_list_api(list_n))
            df = pd.DataFrame(list_data)
            list_final_frame.append(df)
            list_pages = list_data['pageCount']
            list_n=list_n+1
        else:
            list_data = json.loads(get_table_list_api(list_n))
            df = pd.DataFrame(list_data)
            list_final_frame.append(df)
            list_n=list_n+1
    final_list = pd.concat(list_final_frame)        
    return(final_list)





#############################################################
######                TABLE API MODULE                 ######
#############################################################


def call_backup_api(table_id,q_n):
    # Prepare for GET 
    request_headers = {
        #'Authorization': f"{ response_json['token_type'] } { response_json['access_token']}",
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }
    q_size = 500  
    q_param = f'?pageNumber={q_n}&pageSize={q_size}&showbrief=False'
    # Get API data
    q_api_response = requests.get(f"https://api.{instance}/api/v2/flows/datatables/{table_id}/rows{q_param}", headers=request_headers)
    
    # Check response
    if q_api_response.status_code == 200:
        return q_api_response.content

    else:
        print(f"Failure: {str(q_api_response.status_code)} - {q_api_response.reason}")
        return q_api_response.content  


def backup_loop(table_id,table_name):
    back_n=1
    back_pages = 1
    back_final_list = []
    while back_n < back_pages+1:
        if back_n == 1: #first iteration defines number of pages and takes first portion of data
            back_data = json.loads(call_backup_api(table_id,back_n))
            print("Table Processed: ", table_name)
            if 'pageNumber' in back_data.keys():
                back_df = pd.DataFrame(back_data)
                back_final_list.append(back_df)
                back_pages = back_data['pageCount']
                back_n=back_n+1
            else:
                error_code = {back_data['status']:back_data['message']}
                print(error_code)
                return error_code
            
        else:
            back_data = json.loads(call_backup_api(table_id,back_n))
            if 'pageNumber' in back_data.keys():
                back_df = pd.DataFrame(back_data)
                back_final_list.append(back_df)
                back_n=back_n+1
            else:
                error_code = {back_data['status']:back_data['message']}
                print(error_code)
                return error_code
    back_final_frame = pd.concat(back_final_list)
    file_frame = pd.json_normalize(back_final_frame['entities'])
    file_frame.to_csv(f'C:\\Users\\mchrzano\\OneDrive - Capgemini\\Desktop\\DOP\\BackupAPI\\{selected_instance} {table_name} {import_date}.csv', index=False)         
    return('Success')





#############################################################
######                   EXECUTION                     ######
#############################################################

selected_instance = 'LAB'

CLIENT_ID = eval(f'{selected_instance}_ID')
CLIENT_SECRET = eval(f'{selected_instance}_Secret')
instance = eval(f'{selected_instance}_Env')
print("Imported instance - ",selected_instance,instance)

token = get_token()
formated_data = get_table_list()
formated_data = pd.json_normalize(formated_data['entities'])
formated_data = pd.DataFrame(formated_data, columns=['name','id']).assign(status = ('To Be Backed-Up'), import_date = ('')) 

import_date = dt.datetime.now().strftime("%d %m %Y %H %M")

table_processed = 0

for index, item in formated_data.iterrows():
    table_id = item['id']
    table_name = item['name']
    backup_status = item['status']
    func_response = backup_loop(table_id,table_name)
    print(table_id, func_response)
    formated_data.at[index,'status'] = func_response
    formated_data.at[index,'import_date'] = import_date
    table_processed = table_processed+1

formated_data.to_excel(f'C:\\Users\\mchrzano\\OneDrive - Capgemini\\Desktop\\DOP\\BackupAPI\\{selected_instance} !!!STATUS!!! {import_date}.xlsx', index=False)      
print(f"Import Completed. Tables processed: {table_processed}")    