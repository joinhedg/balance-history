import os
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import secrets
import urllib.parse


def load_credentials():
    load_dotenv()  # take environment variables from .env.

    # Database settings
    username = os.environ.get('POSTGRES_USERNAME')
    password = os.environ.get('POSTGRES_PASSWORD')
    hostname = os.environ.get('POSTGRES_HOSTNAME')
    port = os.environ.get('POSTGRES_PORT')
    database = os.environ.get('POSTGRES_DATABASE')
    rapidapi_key = os.environ.get('X_RAPIDAPI_KEY')
    rapidapi_host = os.environ.get('X_RAPIDAPI_HOST')
    lemon_key = os.environ.get('LEMON_KEY')
    eod_key = os.environ.get('EOD_KEY')
    bubble_token = os.environ.get('BUBBLE_TOKEN')
    bubble_base_url = os.environ.get('BUBBLE_BASE_URL')
    api_token = os.environ.get('API_TOKEN')

    return {'username': username,
            'password': password,
            'hostname': hostname,
            'port': port,
            'database': database,
            'rapidapi_key': rapidapi_key,
            'rapidapi_host': rapidapi_host,
            'lemon_key': lemon_key,
            'eod_key': eod_key,
            'bubble_token': bubble_token,
            'bubble_base_url': bubble_base_url,
            'api_token': api_token
            }


def insert_into_table(df, table, envr, if_exists_):
    engine = create_engine(
        f"postgresql://{envr['username']}:{envr['password']}@{envr['hostname']}:{envr['port']}/{envr['database']}")
    conn = engine.connect()
    df.to_sql(table, engine, schema='public', if_exists=if_exists_, index=False)
    conn.close()
    engine.dispose()
    print("exported")


def import_table(table, envr):
    engine = create_engine(
        f"postgresql://{envr['username']}:{envr['password']}@{envr['hostname']}:{envr['port']}/{envr['database']}")
    query = text(f'SELECT * FROM {table}')
    conn = engine.connect()
    dataframe = pd.read_sql(query, con=conn)
    conn.close()
    engine.dispose()
    return dataframe


def get_object_from_bubble(object_name, envr, user_uuid, item_id):
    token = envr['bubble_token']
    base_url = envr['bubble_base_url']
    constraints = f'?constraints=[ {{ "key": "item_id", "constraint_type": "equals", "value": "{item_id}"}}, {{"key": "user_uuid", "constraint_type": "equals", "value": "{user_uuid}"}}, {{"limit": "5000"}}]'
    full_url = base_url + object_name + constraints
    encoded_full_url = urllib.parse.quote(full_url, safe=':/?=[]{},')

    headers = {
        'Authorization': f'Bearer {token}'
    }

    response = requests.get(
        encoded_full_url,
        headers=headers
    ).json()

    df = pd.DataFrame(response['response']['results'])

    # check if 'date' column exists in the dataframe
    if 'date' in df.columns:
        # convert date column to datetime format
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S.%fZ')
        df['date'] = df['date'].dt.tz_localize('Europe/Paris')
    return df


def bulk_export_to_bubble(object_name, envr, body):
    token = envr['bubble_token']
    base_url = envr['bubble_base_url']
    full_url = base_url + object_name + "/bulk"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'text/plain'
    }

    response = requests.post(
        full_url,
        headers=headers,
        data=body
    )
    return {'response': response.text}


def test_api_local():
    env = load_credentials()

    token = env['api_token']
    full_url = 'http://0.0.0.0:8080/trigger_balance_history_calc'
    headers = {
        'Authorization': token,
    }

    response = requests.post(
        full_url,
        headers=headers,
        json={
            'user_uuid': '77b5f941-14cb-4f92-88f8-d111feb41f03',
            'item_id': 7846258
        }
    ).json()

    return response


def gen_secret():
    secret = secrets.token_hex(32)
    return secret


if __name__ == "__main__":
    print(test_api_local())
