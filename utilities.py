import os
import urllib
from functools import wraps

import pandas as pd
import requests
from dotenv import load_dotenv
import secrets
from datetime import datetime
import pytz
from flask import request, jsonify
from matplotlib import pyplot as plt


def load_credentials(test):
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
    bubble_base_url = os.environ.get('BUBBLE_TEST_BASE_URL')
    bridge_client_id = os.environ.get('BRIDGE_TEST_CLIENT_ID')
    bridge_client_secret = os.environ.get('BRIDGE_TEST_CLIENT_SECRET')
    bridge_auth_token = os.environ.get('BRIDGE_AUTH_TOKEN')
    user_uuid = os.environ.get('USER_UUID')
    item_id = os.environ.get('ITEM_ID')
    account_id = os.environ.get('ACCOUNT_ID')

    if test is False:
        bubble_base_url = os.environ.get('BUBBLE_PROD_BASE_URL')
        bridge_client_id = os.environ.get('BRIDGE_PROD_CLIENT_ID')
        bridge_client_secret = os.environ.get('BRIDGE_PROD_CLIENT_SECRET')

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
            'api_token': api_token,
            'bridge_client_id': bridge_client_id,
            'bridge_client_secret': bridge_client_secret,
            'bridge_auth_token': bridge_auth_token,
            'user_uuid': user_uuid,
            'item_id': item_id,
            'account_id': account_id
            }


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


def gen_secret():
    secret = secrets.token_hex(32)
    return secret


def get_data_from_bridge_api_list_accounts(client_id, client_secret, access_token, item_id, limit=500):
    base_url = "https://api.bridgeapi.io/v2/accounts"
    url = f"{base_url}?item_id={item_id}"
    headers = {
        'Bridge-Version': '2021-06-01',
        'Client-Id': client_id,
        'Client-Secret': client_secret,
        'Authorization': f'Bearer {access_token}',
    }

    all_data = []

    while url:
        response = requests.get(url, headers=headers, params={'limit': limit})
        data = response.json()

        if response.status_code == 200:
            all_data.extend(data['resources'])

            # Check if there is a next page
            if data['pagination']['next_uri'] is not None:
                url = base_url + data['pagination']['next_uri']
            else:
                url = None
        else:
            print(f"Error: {data['error']}")
            url = None

    df = pd.DataFrame(all_data)

    # Get the current time in Europe/Paris timezone
    paris_tz = pytz.timezone('Europe/Paris')
    current_time = datetime.now(paris_tz)

    # Add the new column to df
    df['date'] = current_time
    return df


def get_data_from_bridge_api_list_transactions_by_account(client_id, client_secret,
                                                          access_token, account_id,
                                                          limit=500,
                                                          until_date=None):
    base_url = f"https://api.bridgeapi.io/v2/accounts/{account_id}/transactions"
    url = f"{base_url}"
    if until_date is not None:
        url += f"&until={until_date}"

    headers = {
        'Bridge-Version': '2021-06-01',
        'Client-Id': client_id,
        'Client-Secret': client_secret,
        'Authorization': f'Bearer {access_token}',
    }

    all_data = []

    while url:
        response = requests.get(url, headers=headers, params={'limit': limit})
        data = response.json()
        if response.status_code == 200:
            all_data.extend(data['resources'])

            # Check if there is a next page
            next_page = data['pagination']['next_uri']
            test = 1
            if next_page is not None:
                url = base_url + next_page
            else:
                url = None
        else:
            print(f"Error: {data['error']}")
            url = None
    df = pd.DataFrame(all_data)
    # df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

    return df


def plot_history(results):
    # Plotting code
    account_ids = results['id'].unique()
    for account_id in account_ids:
        df_account = results[results['id'] == account_id]

        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()

        # Plot balance as a line
        ax1.plot(df_account['date'], df_account['balance'], label='Balance')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Balance')
        ax1.set_title(f'Balance and Amount over Time for Account {account_id}')

        # Plot amount as bars
        ax2.bar(df_account['date'], df_account['total_daily_amount'], alpha=0.5, label='Amount')
        ax2.set_ylabel('Amount')

        # Combine the legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.show()


def amount_in(amount):
    if amount > 0:
        return amount
    else:
        return 0


def amount_out(amount):
    if amount < 0:
        return amount * -1
    else:
        return 0


def get_object_from_bubble(object_name, envr):
    token = envr['bubble_token']
    base_url = envr['bubble_base_url']
    full_url = base_url + object_name

    headers = {
        'Authorization': f'Bearer {token}'
    }

    # Initialize
    df_list = []
    cursor = 0
    remaining = 1  # a non-zero value
    limit = 100

    while remaining > 0:
        response = requests.get(
            full_url,
            headers=headers,
            params={
                'limit': limit,
                'cursor': cursor
            }
        ).json()

        # Append the results to your DataFrame
        df_list.append(pd.DataFrame(response['response']['results']))

        # Update remaining and count
        remaining = response['response']['remaining']
        count = response['response']['count']

        # Update cursor position
        cursor += count

    df = pd.concat(df_list, ignore_index=True)

    return df


def test_api_local(env):
    full_url = 'http://0.0.0.0:8080/trigger_balance_history_calc'
    headers = {
        'Authorization': env['api_token'],
    }

    response = requests.post(
        full_url,
        headers=headers,
        json={
            "user_uuid": env['user_uuid'],
            "item_id": env['item_id'],
            "bridge_token": env['bridge_auth_token'],
            "test": True}
    ).json()

    return response


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing'}), 403

        try:
            env = load_credentials(True)
            if not token == env['api_token']:
                raise Exception("Invalid Token")
        except:
            return jsonify({'message': 'Token is invalid'}), 403

        return f(*args, **kwargs)

    return decorated


if __name__ == '__main__':
    env = load_credentials(True)
    # result_df = get_data_from_bridge_api_list_accounts(
    #     env['BRIDGE_TEST_CLIENT_ID'],
    #     env['BRIDGE_TEST_CLIENT_SECRET'],
    #     access_token="",
    #     item_id=
    # )

    result_df = get_data_from_bridge_api_list_transactions_by_account(
        env['bridge_client_id'],
        env['bridge_client_secret'],
        access_token=env['bridge_auth_token'],
        account_id=env['account_id'])

    # result_df = get_object_from_bubble("bridge_categories", envr=env)

    print(result_df)
