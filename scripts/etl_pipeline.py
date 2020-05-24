from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import json
import pendulum
import pandas as pd
import os
from pymongo import MongoClient
tz = pendulum.timezone('Asia/Jakarta')

PROJECT_NAME = 'studious-loader-272905'
DATASET_NAME = 'octopus_data_warehouse_dev'
LOGS = 'logs'
LOGS_TABLE = 'etl_pipeline'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/sayurbox-data/Documents/Octopus/credentials/studious-loader-272905-82fbeaf77a2d.json'

def insert_into_bq(df, table_name):

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(DATASET_NAME)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig()
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, DATASET_NAME, table_name))

'''
def insert_into_bq(df, table_name):

    pbq.to_gbq(df, '{}.{}'.format(DATASET_NAME, table_name), project_id=PROJECT_NAME, if_exists='append')
    print("Loaded {} rows into {}:{}.".format(df.shape[0], DATASET_NAME, table_name))
'''

def check_table(table_name):

    client = bigquery.Client()
    query_job = client.query(f"""
        SELECT *
        FROM `{PROJECT_NAME}.{DATASET_NAME}.__TABLES_SUMMARY__`
        WHERE table_id = '{table_name}'
        """)

    results = query_job.result()
    return True if len([x for x in results]) > 0 else False

def check_exist(table_name, col, condition):

    client = bigquery.Client()
    query_job = client.query(f"""
        SELECT {col}
        FROM `{PROJECT_NAME}.{DATASET_NAME}.{table_name}`
        WHERE {col} in ({condition})
        """)

    results = query_job.result()
    return [x[col] for x in results]

def read_orders(orders):
    '''Function to read both orders and cancelled_orders'''

    datapoints = []
    data = orders.find()
    rows = orders.count()

    #print('Total documents:', rows)

    for idx, order in enumerate(data):
        
        try:
            _id = order['id'] if 'id' in order else None
            order_id = order['order_id'] if 'order_id' in order else None
            updated_at = order['updated_at'] if 'updated_at' in order else None
            timestamp_package = order['timestamp_package_delivered'] if 'timestamp_package_delivered' in order else None
            order_type = order['order_type'] if 'order_type' in order else None
            total = order['total'] if 'total' in order else None
            
            user_id = order['user']['id'] if 'user' in order and 'id' in order['user'] else None
            user_name = order['user']['name'] if 'user' in order and 'name' in order['user'] else None
            user_email = order['user']['email'] if 'user' in order and 'email' in order['user'] else None
            user_type = order['user']['user_type'] if 'user' in order and 'user_type' in order['user'] else None
            
            receiver_id = order['user_receiver']['id'] if 'user_receiver' in order and 'id' in order['user_receiver'] else None
            receiver_name = order['user_receiver']['name'] if 'user_receiver' in order and 'name' in order['user_receiver'] else None
            receiver_email = order['user_receiver']['email'] if 'user_receiver' in order and 'email' in order['user_receiver'] else None
            receiver_type = order['user_receiver']['user_type'] if 'user_receiver' in order and 'user_type' in order['user_receiver'] else None
            
            country = order['country'] if 'country' in order else None
            province = order['province'] if 'province' in order else None
            city = order['city'] if 'city' in order else None
            district = order['district'] if 'district' in order else None
            village = order['village'] if 'village' in order else None
            
            latitude = order['latitude'] if 'latitude' in order else None
            longitude = order['longitude'] if 'longitude' in order else None
            is_accepted = order['is_accepted'] if 'is_accepted' in order else None
            is_arrived = order['is_arrived'] if 'is_arrived' in order else None
            is_picked = order['is_picked'] if 'is_picked' in order else None
            is_settled = order['is_settled'] if 'is_settled' in order else None
            
            datapoint = (_id, order_id, updated_at, timestamp_package, order_type, total,
                        user_id, user_name, user_email, user_type,
                        receiver_id, receiver_name, receiver_email, receiver_type,
                        country, province, city, district, village, latitude, longitude,
                        is_accepted, is_arrived, is_picked, is_settled)
            datapoints.append(datapoint)

        except:
            print('Error', order['id'])
    
    df = pd.DataFrame(datapoints, columns=['id', 'order_id', 'timestamp', 'data_timestamp', 'order_type', 'amount',
                                        'user_id', 'user_name', 'user_email', 'user_type',
                                        'receiver_id', 'receiver_name', 'receiver_email', 'receiver_type',
                                        'country', 'province', 'city', 'district', 'village', 'latitude', 'longitude',
                                        'is_accepted', 'is_arrived', 'is_picked', 'is_settled'])
    df['id'] = df['id'].astype(int)
    df['order_id'] = df['order_id'].astype(str)
    df['timestamp'] = pd.to_datetime(df['timestamp']).values.astype('datetime64[ms]')
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp']).values.astype('datetime64[ms]')
    df['order_type'] = df['order_type'].astype(str)
    df['amount'] = df['amount'].astype(float)
    df['user_id'] = df['user_id'].astype(str)
    df['user_name'] = df['user_name'].str.lower().astype(str)
    df['user_email'] = df['user_email'].astype(str)
    df['user_type'] = df['user_type'].str.lower().astype(str)
    df['receiver_id'] = df['receiver_id'].astype(float)
    df['receiver_name'] = df['receiver_name'].str.lower().astype(str)
    df['receiver_email'] = df['receiver_email'].astype(str)
    df['receiver_type'] = df['receiver_type'].str.lower().astype(str)
    df['country'] = df['country'].str.lower().astype(str)
    df['province'] = df['province'].str.lower().astype(str)
    df['city'] = df['city'].str.lower().astype(str)
    df['district'] = df['district'].str.lower().astype(str)
    df['village'] = df['village'].str.lower().astype(str)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['is_accepted'] = df['is_accepted'].astype(bool)
    df['is_arrived'] = df['is_arrived'].astype(bool)
    df['is_picked'] = df['is_picked'].astype(bool)
    df['is_settled'] = df['is_settled'].astype(bool)

    if check_table('orders') and df.shape[0] > 0:
        list_id = '"' + '","'.join(df['order_id'].tolist()) + '"'
        existed = check_exist('orders', 'order_id', list_id)
        #print(len(existed), ' row(s) existed...')
        df = df[~df['order_id'].isin(existed)]
    #print('Added', df.shape[0], 'rows!')

    return df

def read_settlements(scav_orders):
    '''Function to read scavanger_orders'''

    datapoints = []
    data = scav_orders.find()
    rows = scav_orders.count()

    #print('Total documents:', rows)

    for idx, order in enumerate(data):
        
        try:
            _id = order['id'] if 'id' in order else None
            order_id = order['order_id'] if 'order_id' in order else None
            updated_at = order['updated_at'] if 'updated_at' in order else None
            timestamp_package = order['timestamp_package_delivered'] if 'timestamp_package_delivered' in order else None
            order_type = order['order_type'] if 'order_type' in order else None
            total = order['total'] if 'total' in order else None
            
            country = order['country'] if 'country' in order else None
            province = order['province'] if 'province' in order else None
            city = order['city'] if 'city' in order else None
            district = order['district'] if 'district' in order else None
            village = order['village'] if 'village' in order else None
            
            latitude = order['latitude'] if 'latitude' in order else None
            longitude = order['longitude'] if 'longitude' in order else None
            is_accepted = order['is_accepted'] if 'is_accepted' in order else None
            is_arrived = order['is_arrived'] if 'is_arrived' in order else None
            is_picked = order['is_picked'] if 'is_picked' in order else None
            is_settled = order['is_settled'] if 'is_settled' in order else None
            
            user = None
            receiver = None
            if 'user' in order:
                if order['user']['user_type']=='Scavanger':
                    user = order['user']
                elif order['user']['user_type']=='Waste Bank':
                    receiver = order['user']
            
            if 'user_receiver' in order:
                if order['user_receiver']['user_type']=='Scavanger':
                    user = order['user_receiver']
                elif order['user_receiver']['user_type']=='Waste Bank':
                    receiver = order['user_receiver']
                
            user_id, user_name, user_email, user_type = None, None, None, None
            if user != None:
                user_id = user['id']
                user_name = user['name']
                user_email = user['email']
                user_type = user['user_type']
            
            receiver_id, receiver_name, receiver_email, receiver_type = None, None, None, None
            if receiver != None:
                receiver_id = receiver['id']
                receiver_name = receiver['name']
                receiver_email = receiver['email']
                receiver_type = receiver['user_type']
            
            datapoint = (_id, order_id, updated_at, timestamp_package, order_type, total,
                        user_id, user_name, user_email, user_type,
                        receiver_id, receiver_name, receiver_email, receiver_type,
                        country, province, city, district, village, latitude, longitude,
                        is_accepted, is_arrived, is_picked, is_settled)
            datapoints.append(datapoint)

        except:
            print('Error', order['id'])
        
    df = pd.DataFrame(datapoints, columns=['id', 'order_id', 'timestamp', 'data_timestamp', 'order_type', 'amount',
                                        'user_id', 'user_name', 'user_email', 'user_type',
                                        'receiver_id', 'receiver_name', 'receiver_email', 'receiver_type',
                                        'country', 'province', 'city', 'district', 'village', 'latitude', 'longitude',
                                        'is_accepted', 'is_arrived', 'is_picked', 'is_settled'])
    df['id'] = df['id'].astype(int)
    df['order_id'] = df['order_id'].astype(str)
    df['timestamp'] = pd.to_datetime(df['timestamp']).values.astype('datetime64[ms]')
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp']).values.astype('datetime64[ms]')
    df['order_type'] = df['order_type'].astype(str)
    df['amount'] = df['amount'].astype(float)
    df['user_id'] = df['user_id'].astype(str)
    df['user_name'] = df['user_name'].str.lower().astype(str)
    df['user_email'] = df['user_email'].astype(str)
    df['user_type'] = df['user_type'].str.lower().astype(str)
    df['receiver_id'] = df['receiver_id'].astype(float)
    df['receiver_name'] = df['receiver_name'].str.lower().astype(str)
    df['receiver_email'] = df['receiver_email'].astype(str)
    df['receiver_type'] = df['receiver_type'].str.lower().astype(str)
    df['country'] = df['country'].str.lower().astype(str)
    df['province'] = df['province'].str.lower().astype(str)
    df['city'] = df['city'].str.lower().astype(str)
    df['district'] = df['district'].str.lower().astype(str)
    df['village'] = df['village'].str.lower().astype(str)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['is_accepted'] = df['is_accepted'].astype(bool)
    df['is_arrived'] = df['is_arrived'].astype(bool)
    df['is_picked'] = df['is_picked'].astype(bool)
    df['is_settled'] = df['is_settled'].astype(bool)

    if check_table('settled_orders') and df.shape[0] > 0:
        list_id = '"' + '","'.join(df['order_id'].tolist()) + '"'
        existed = check_exist('settled_orders', 'order_id', list_id)
        #print(len(existed), ' row(s) existed...')
        df = df[~df['order_id'].isin(existed)]
    #print('Added', df.shape[0], 'rows!')

    return df

def read_items(orders, cart_label):
    '''Function to read items from both orders and cancelled_orders'''

    datapoints = []
    data = orders.find()
    rows = orders.count()

    #print('Total documents:', rows)

    for idx, order in enumerate(data):
        
        try:
            if cart_label in order:
                n = len(order[cart_label])
                for i in range(n):
                    item = order[cart_label][i]
                    
                    _id = item['id'] if 'id' in item else None
                    order_list_id = item['order_list_id'] if 'order_list_id' in item else None
                    order_id = item['order_id'] if 'order_id' in item else None
                    quantity = item['quantity'] if 'quantity' in item else None
                    amount = item['amount'] if 'amount' in item else None
                    subcategory_id = item['subcategory']['id'] if 'subcategory' in item and 'id' in item['subcategory'] else None
                    subcategory_name = item['subcategory']['name'] if 'subcategory' in item and 'name' in item['subcategory'] else None
                    subcategory_desc = item['subcategory']['description'] if 'subcategory' in item and 'description' in item['subcategory'] else None
                    subcategory_img = item['subcategory']['image'] if 'subcategory' in item and 'image' in item['subcategory'] else None
                    category_id = item['subcategory']['category']['id'] if 'subcategory' in item and 'category' in item['subcategory'] and 'id' in item['subcategory']['category'] else None
                    category_name = item['subcategory']['category']['name'] if 'subcategory' in item and 'category' in item['subcategory'] and 'name' in item['subcategory']['category'] else None
                    category_img = item['subcategory']['category']['image'] if 'subcategory' in item and 'category' in item['subcategory'] and 'image' in item['subcategory']['category'] else None
                    is_placed = item['is_placed'] if 'is_placed' in item else None
                    
                    datapoint = (_id, order_list_id, order_id, quantity, amount,
                                subcategory_id, subcategory_name, subcategory_desc, subcategory_img,
                                category_id, category_name, category_img, is_placed)
                    datapoints.append(datapoint)

        except:
            print('Error', order['id'])
            
    df = pd.DataFrame(datapoints, columns=['item_id', 'order_list_id', 'order_id', 'quantity', 'amount',
                                        'subcategory_id', 'subcategory_name', 'subcategory_desc', 'subcategory_img',
                                        'category_id', 'category_name', 'category_img', 'is_placed'])
    df['subtotal'] = df['quantity'] * df['amount']
    df = df[['item_id', 'order_list_id', 'order_id', 'quantity', 'amount', 'subtotal',
                                        'subcategory_id', 'subcategory_name', 'subcategory_desc', 'subcategory_img',
                                        'category_id', 'category_name', 'category_img', 'is_placed']]

    df['item_id'] = df['item_id'].astype(int)
    df['order_list_id'] = df['item_id'].astype(int)
    df['order_id'] = df['order_id'].astype(str)
    df['quantity'] = df['quantity'].astype(int)
    df['amount'] = df['amount'].astype(float)
    df['subtotal'] = df['subtotal'].astype(float)
    df['subcategory_id'] = df['subcategory_id'].astype(int)
    df['subcategory_name'] = df['subcategory_name'].astype(str)
    df['subcategory_desc'] = df['subcategory_desc'].astype(str)
    df['subcategory_img'] = df['subcategory_img'].astype(str)
    df['category_id'] = df['category_id'].astype(int)
    df['category_name'] = df['category_name'].astype(str)
    df['category_img'] = df['category_img'].astype(str)
    df['is_placed'] = df['is_placed'].astype(bool)

    if check_table('items') and df.shape[0] > 0:
        list_id_str = [str(i) for i in df['item_id'].tolist()]
        list_id = ', '.join(list_id_str)
        existed = check_exist('items', 'item_id', list_id)
        df = df[~df['item_id'].isin(existed)]
        #print(len(existed), ' row(s) existed...')
    #print('Added', df.shape[0], 'rows!')

    return df

def read_settled_items(scav_orders):
    '''Function to read items from settlements'''

    datapoints = []
    data = scav_orders.find()
    rows = scav_orders.count()

    #print('Total documents:', rows)

    for idx, order in enumerate(data):
        
        try:
            if 'carts' in order:
                n = len(order['carts'])
                for i in range(n):
                    item = order['carts'][i]
                    
                    _id = item['id'] if 'id' in item else None
                    order_list_id = item['order_list_id'] if 'order_list_id' in item else None
                    order_id = item['order_id'] if 'order_id' in item else None
                    quantity = item['quantity'] if 'quantity' in item else None
                    amount = item['amount'] if 'amount' in item else None
                    subcategory_id = item['subcategory']['id'] if 'subcategory' in item and 'id' in item['subcategory'] else None
                    subcategory_name = item['subcategory']['name'] if 'subcategory' in item and 'name' in item['subcategory'] else None
                    subcategory_desc = item['subcategory']['description'] if 'subcategory' in item and 'description' in item['subcategory'] else None
                    subcategory_img = item['subcategory']['image'] if 'subcategory' in item and 'image' in item['subcategory'] else None
                    category_id = item['subcategory']['category']['id'] if 'subcategory' in item and 'category' in item['subcategory'] and 'id' in item['subcategory']['category'] else None
                    category_name = item['subcategory']['category']['name'] if 'subcategory' in item and 'category' in item['subcategory'] and 'name' in item['subcategory']['category'] else None
                    category_img = item['subcategory']['category']['image'] if 'subcategory' in item and 'category' in item['subcategory'] and 'image' in item['subcategory']['category'] else None
                    is_placed = item['is_placed'] if 'is_placed' in item else None
                    
                    datapoint = (_id, order_list_id, order_id, quantity, amount,
                                subcategory_id, subcategory_name, subcategory_desc, subcategory_img,
                                category_id, category_name, category_img, is_placed)
                    datapoints.append(datapoint)

        except:
            print('Error', order['id'])
            
    df = pd.DataFrame(datapoints, columns=['item_id', 'order_list_id', 'order_id', 'quantity', 'amount',
                                        'subcategory_id', 'subcategory_name', 'subcategory_desc', 'subcategory_img',
                                        'category_id', 'category_name', 'category_img', 'is_placed'])
    df['subtotal'] = df['quantity'] * df['amount']
    df = df[['item_id', 'order_list_id', 'order_id', 'quantity', 'amount', 'subtotal',
                                        'subcategory_id', 'subcategory_name', 'subcategory_desc', 'subcategory_img',
                                        'category_id', 'category_name', 'category_img', 'is_placed']]

    df['item_id'] = df['item_id'].astype(int)
    df['order_list_id'] = df['item_id'].astype(int)
    df['order_id'] = df['order_id'].astype(str)
    df['quantity'] = df['quantity'].astype(int)
    df['amount'] = df['amount'].astype(float)
    df['subtotal'] = df['subtotal'].astype(float)
    df['subcategory_id'] = df['subcategory_id'].astype(int)
    df['subcategory_name'] = df['subcategory_name'].astype(str)
    df['subcategory_desc'] = df['subcategory_desc'].astype(str)
    df['subcategory_img'] = df['subcategory_img'].astype(str)
    df['category_id'] = df['category_id'].astype(int)
    df['category_name'] = df['category_name'].astype(str)
    df['category_img'] = df['category_img'].astype(str)
    df['is_placed'] = df['is_placed'].astype(bool)

    if check_table('settled_items') and df.shape[0] > 0:
        list_id_str = [str(i) for i in df['item_id'].tolist()]
        list_id = ', '.join(list_id_str)
        existed = check_exist('settled_items', 'item_id', list_id)
        df = df[~df['item_id'].isin(existed)]
        #print(len(existed), ' row(s) existed...')
    #print('Added', df.shape[0], 'rows!')
    
    return df

def read_users(users):
    '''Function to read users'''

    datapoints = []
    data = users.find()
    rows = users.count()

    #print('Total documents:', rows)

    for idx, user in enumerate(data):
        
        try:
            
            _id = user['id'] if 'id' in user else None
            uuid = user['uuid'] if 'uuid' in user else None
            user_type = user['user_type'] if 'user_type' in user else None
            name = user['name'] if 'name' in user else None
            gender = user['gender'] if 'gender' in user else None
            email = user['email'] if 'email' in user else None
            nik = user['nik'] if 'nik' in user else None
            updated_at = user['updated_at'] if 'updated_at' in user else None
            timestamp_package = user['timestamp_package_delivered'] if 'timestamp_package_delivered' in user else None
            
            datapoint = (_id, uuid, user_type,
                        name, gender, email, nik, 
                        updated_at, timestamp_package)
            datapoints.append(datapoint)

        except:
            print('Error', user['id'])
          
    df = pd.DataFrame(datapoints, columns=['user_id', 'uuid', 'type',
                                        'name', 'gender', 'email', 'nik',
                                        'registered_date', 'data_timestamp'])
    
    df['user_id'] =  df['user_id'].astype(int)
    df['uuid'] =  df['uuid'].astype(str)
    df['type'] =  df['type'].str.lower().astype(str)
    df['name'] =  df['name'].str.lower().astype(str)
    df['gender'] =  df['gender'].astype(str)
    df['email'] =  df['email'].astype(str)
    df['nik'] =  df['nik'].astype(str)
    df['registered_date'] = pd.to_datetime(df['registered_date']).dt.date
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp']).values.astype('datetime64[ms]')
    df['verified'] = False
    if df.shape[0] > 0:
        df.loc[~df['nik'].isnull(), 'verified'] = True
    df['verified'] =  df['verified'].astype(bool)

    if check_table('users') and df.shape[0] > 0:
        list_id = '"' + '","'.join(df['uuid'].tolist()) + '"'
        existed = check_exist('users', 'uuid', list_id)
        df = df[~df['uuid'].isin(existed)]
        #print(len(existed), ' row(s) existed...')
    #print('Added', df.shape[0], 'rows!')

    return df

def read_wallets(wallets):

    datapoints = []
    data = wallets.find()
    rows = wallets.count()

    #print('Total documents:', rows)

    for idx, wallet in enumerate(data):
        
        try:
            
            _id = wallet['id'] if 'id' in wallet else None
            user_id = wallet['user']['id'] if 'user' in wallet and 'id' in wallet['user'] else None
            user_uuid = wallet['user']['uuid'] if 'user' in wallet and 'uuid' in wallet['user'] else None
            user_name = wallet['user']['name'] if 'user' in wallet and 'name' in wallet['user'] else None
            user_email = wallet['user']['email'] if 'user' in wallet and 'email' in wallet['user'] else None
            user_type = wallet['user']['user_type'] if 'user' in wallet and 'user_type' in wallet['user'] else None
            balance = wallet['balance'] if 'balance' in wallet else None
            updated_at = wallet['updated_at'] if 'updated_at' in wallet else None
            timestamp_package = wallet['timestamp_package_delivered'] if 'timestamp_package_delivered' in wallet else None
            
            datapoint = (_id, user_id, user_uuid, user_name, user_email, user_type,
                        balance, updated_at, timestamp_package)
            datapoints.append(datapoint)

        except:
            print('Error', wallet['id'])
            
    df = pd.DataFrame(datapoints, columns=['wallet_id', 'user_id', 'user_uuid', 'user_name', 'user_email', 'user_type',
                                                'balance', 'timestamp', 'data_timestamp'])

    df['wallet_id'] = df['wallet_id'].astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['user_uuid'] = df['user_uuid'].astype(str)
    df['user_name'] = df['user_name'].str.lower().astype(str)
    df['user_email'] = df['user_email'].astype(str)
    df['user_type'] = df['user_type'].str.lower().astype(str)
    df['balance'] = df['balance'].astype(float)
    df['timestamp'] = pd.to_datetime(df['timestamp']).values.astype('datetime64[ms]')
    df['data_timestamp'] = pd.to_datetime(df['data_timestamp']).values.astype('datetime64[ms]')

    if check_table('wallets') and df.shape[0] > 0:
        list_id_str = [str(i) for i in df['wallet_id'].tolist()]
        list_id = ', '.join(list_id_str)
        existed = check_exist('wallets', 'wallet_id', list_id)
        df = df[~df['wallet_id'].isin(existed)]
        #print(len(existed), ' row(s) existed...')
    #print('Added', df.shape[0], 'rows!')

    return df

def write_logs(start_exec, end_exec, table_name, num_rows):

    data = [(start_exec, end_exec, table_name, num_rows, 'manual')]
    df = pd.DataFrame(data, columns=['start_ts', 'end_ts', 'table_name', 'num_rows', 'channel'])

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(LOGS)
    table_ref = dataset_ref.table(LOGS_TABLE)

    job_config = bigquery.LoadJobConfig()
    job = bigquery_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print("Logged {} rows into {}:{}.".format(job.output_rows, LOGS, LOGS_TABLE))

def read_data():

    print('Started executing at', datetime.now())

    client = MongoClient('mongodb+srv://data_analytics:Q3mLXUIkBQ7tL2NT@da-orders-test-1s9ts.gcp.mongodb.net/test?retryWrites=true&w=majority')

    db = client['octopus_prod']
    orders = db['orders']
    scav_orders = db['scavenger_orders']
    cancelled_orders = db['cancelled_orders']
    categories = db['categories']
    subcategories = db['subcategories']
    logins = db['user_logins']
    logouts = db['user_logouts']
    users = db['users']
    wallets = db['wallets']

    print('+++ ORDERS +++')
    #orders
    start_exec = datetime.now()
    df_accepted = read_orders(orders)
    df_accepted['status'] = 'completed'
    df_cancelled = read_orders(cancelled_orders)
    df_cancelled['status'] = 'cancelled'
    df_orders = df_accepted.append(df_cancelled)
    df_orders['status'] = df_orders['status'].astype(str)
    df_orders = df_orders.fillna({'country': 'unknown', 'province': 'unknown', 'city': 'unknown', 'district': 'unknown', 'village': 'unknown'})
    if df_orders.shape[0] > 0:
        insert_into_bq(df_orders, 'orders')
        write_logs(start_exec, datetime.now(), 'orders', df_orders.shape[0])

    
    print('+++ SETTLED ORDERS +++')
    #settlements
    start_exec = datetime.now()
    df_settlements = read_settlements(scav_orders)
    if df_settlements.shape[0] > 0:
        insert_into_bq(df_settlements, 'settled_orders')
        write_logs(start_exec, datetime.now(), 'settled_orders', df_settlements.shape[0])

    print('+++ ITEMS +++')
    #order-items
    start_exec = datetime.now()
    df_item_accepted = read_items(orders, 'carts')
    df_item_cancelled = read_items(cancelled_orders, 'cancelled_order_carts')
    df_items = df_item_accepted.append(df_item_cancelled)
    if df_items.shape[0] > 0:
        insert_into_bq(df_items, 'items')
        write_logs(start_exec, datetime.now(), 'items', df_items.shape[0])

    print('+++ SETTLED ITEMS +++')
    #settled-items
    start_exec = datetime.now()
    df_settled_items = read_settled_items(scav_orders)
    if df_settled_items.shape[0] > 0:
        insert_into_bq(df_settled_items, 'settled_items')
        write_logs(start_exec, datetime.now(), 'settled_items', df_settled_items.shape[0])

    print('+++ USERS +++')
    #users
    start_exec = datetime.now()
    df_users = read_users(users)
    if df_users.shape[0] > 0:
        insert_into_bq(df_users, 'users')
        write_logs(start_exec, datetime.now(), 'users', df_users.shape[0])

    print('+++ WALLETS +++')
    #wallets
    start_exec = datetime.now()
    df_wallets = read_wallets(wallets)
    if df_wallets.shape[0] > 0:
        insert_into_bq(df_wallets, 'wallets')
        write_logs(start_exec, datetime.now(), 'wallets', df_wallets.shape[0])

    print('Finished executing at', datetime.now())

if __name__ == '__main__':

    read_data()