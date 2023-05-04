import json
import pandas as pd
from utilities import load_credentials, get_object_from_bubble, bulk_export_to_bubble
from flask import Flask, request, jsonify
from functools import wraps

app = Flask(__name__)


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing'}), 403

        try:
            env = load_credentials()
            if not token == env['api_token']:
                raise Exception("Invalid Token")
        except:
            return jsonify({'message': 'Token is invalid'}), 403

        return f(*args, **kwargs)

    return decorated


def history_calculation(user_uuid, item_id):
    env = load_credentials()

    df_account = get_object_from_bubble("bridge_account", env, user_uuid=user_uuid, item_id=item_id)
    df_account = df_account[(df_account['user_uuid'] == user_uuid) & (df_account['item_id'] == item_id)]
    df_account.rename(columns={'id': 'account_id'}, inplace=True)
    df_account_unique = df_account[
        ['account_id', 'bank_id', 'currency_code', 'iban', 'is_pro', 'name', 'item_id', 'user_uuid']].drop_duplicates()
    df_account_grouped_min_date = df_account.groupby(['item_id', 'account_id', 'user_uuid']).agg(
        {'date': 'min'}).reset_index()

    df_transactions = get_object_from_bubble("bridge_transactions", env, user_uuid=user_uuid, item_id=item_id)
    df_transactions_grouped_min_date = df_transactions.groupby(['item_id', 'account_id', 'user_uuid']).agg(
        {'date': 'min'}).reset_index()

    # Group by 'item_id', 'account_id', 'user_uuid', and 'date', and aggregate the sum of 'amount'
    df_transactions_grouped_sum_amount = df_transactions.groupby(['item_id', 'account_id', 'user_uuid', 'date']).agg(
        {'amount': 'sum'}).reset_index()

    # Merge grouped_df and grouped_df_min_date on 'account_id', 'item_id', and 'user_uuid'
    merged_df = pd.merge(df_account_grouped_min_date, df_transactions_grouped_min_date,
                         on=['account_id', 'item_id', 'user_uuid'],
                         suffixes=('_account_min', '_transactions_min'))

    merged_df = merged_df[merged_df['date_account_min'] > merged_df['date_transactions_min']]

    # Initialize an empty dataframe to store the results
    results = pd.DataFrame(
        columns=['account_id', 'item_id', 'user_uuid', 'date', 'total_history_amount', 'last_balance'])

    for index, row in merged_df.iterrows():
        min_transac_date = row['date_transactions_min']
        min_account_date = row['date_account_min']
        timestamp_3_months_prior = min_account_date - pd.DateOffset(months=3)
        start_date = min(min_transac_date, timestamp_3_months_prior)
        end_date = min_account_date

        # Generate a date range between the start and end dates
        date_range = pd.date_range(start=start_date.normalize(), end=end_date.normalize(), freq='D', tz='Europe/Paris')

        # Create a DataFrame with the date column
        temp_df = pd.DataFrame({'date': date_range})
        temp_df['date'] = temp_df['date'].dt.tz_convert('UTC')
        temp_df['item_id'] = row['item_id']
        temp_df['account_id'] = row['account_id']
        temp_df['user_uuid'] = row['user_uuid']

        # Merge grouped_df and grouped_df_min_date on 'account_id', 'item_id', and 'user_uuid'
        temp_df = pd.merge(temp_df, df_account_unique,
                           on=['account_id', 'item_id', 'user_uuid']
                           )

        def history_amount(history_date):
            # Calculate the total amount per day for the same 'account_id', 'item_id', and 'user_uuid'
            temp_df_ammout = df_transactions_grouped_sum_amount[
                (df_transactions_grouped_sum_amount['account_id'] == row['account_id']) &
                (df_transactions_grouped_sum_amount['item_id'] == row['item_id']) &
                (df_transactions_grouped_sum_amount['user_uuid'] == row['user_uuid']) &
                (df_transactions_grouped_sum_amount['date'] >= history_date)]
            return temp_df_ammout['amount'].sum()

        temp_df['total_history_amount'] = temp_df.apply(lambda x: history_amount(x['date']), axis=1)

        temp_df_balance = df_account[
            (df_account['account_id'] == row['account_id']) &
            (df_account['item_id'] == row['item_id']) &
            (df_account['user_uuid'] == row['user_uuid']) &
            (df_account['date'] == row['date_account_min'])]

        last_balance = temp_df_balance['balance'].sum()

        temp_df['last_balance'] = last_balance

        # Append the results to the 'results' dataframe
        results = pd.concat([results, temp_df], ignore_index=True)

    results['balance'] = (results['last_balance'] - results['total_history_amount']).round(2)
    results.drop(columns=['total_history_amount', 'last_balance'], inplace=True)
    results.rename(columns={'account_id': 'id'}, inplace=True)
    test = results.empty
    if results.empty is False:
        results['date'] = results['date'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        results = results.to_dict('records')

        output_body = ""
        for result in results:
            output_body += json.dumps(result) + '\n'
        response = bulk_export_to_bubble("bridge_account", envr=env, body=output_body)
        return response
    else:
        return 'everything up to date'


@app.route('/trigger_balance_history_calc', methods=['POST'])
@token_required
def trigger_balance_history_calc():
    # Récupérez les arguments de la requête POST
    data = request.json
    user_uuid = data.get('user_uuid')
    item_id = data.get('item_id')

    # Passez les arguments à la fonction
    result = history_calculation(user_uuid=user_uuid, item_id=item_id)

    return jsonify({"result": result})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
    #result = history_calculation(user_uuid='77b5f941-14cb-4f92-88f8-d111feb41f03', item_id=7846258)
    # print(result)
