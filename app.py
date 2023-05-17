import json
import pandas as pd
from utilities import load_credentials, bulk_export_to_bubble, \
    get_data_from_bridge_api_list_accounts, get_data_from_bridge_api_list_transactions_by_account, amount_in, \
    amount_out, get_object_from_bubble, token_required
from flask import Flask, request, jsonify

app = Flask(__name__)


def history_calculation(item_id, user_uuid, bridge_token, test):
    # Load env variables
    env = load_credentials(test)

    # Load categories and transform
    df_categories = get_object_from_bubble("bridge_categories", envr=env)
    df_categories = df_categories[['id', 'name', 'color', 'parent_name']]
    df_categories.rename(columns={'id': 'category_id'}, inplace=True)

    # Fetch account data and transform
    df_accounts = get_data_from_bridge_api_list_accounts(
        env['bridge_client_id'],
        env['bridge_client_secret'],
        access_token=bridge_token,
        item_id=item_id
    )
    df_accounts.rename(columns={'id': 'account_id'}, inplace=True)

    # Group by account to find the min balance date
    df_account_grouped_min_date = df_accounts.groupby(['item_id', 'account_id']).agg({'date': 'min'}).reset_index()

    # Fetch transactions for each account
    df_all_transactions = pd.DataFrame()
    for account_id in df_accounts['account_id'].unique():
        df_transactions = get_data_from_bridge_api_list_transactions_by_account(
            env['bridge_client_id'],
            env['bridge_client_secret'],
            access_token=bridge_token,
            account_id=account_id
        )
        df_all_transactions = pd.concat([df_transactions, df_all_transactions])

    # Add item_id
    df_all_transactions['item_id'] = item_id

    # Convert to datetime
    df_all_transactions['date'] = pd.to_datetime(df_all_transactions['date'], format='%Y-%m-%d')
    df_all_transactions['date'] = df_all_transactions['date'].dt.tz_localize('Europe/Paris')
    # Filter transactions
    df_all_transactions = df_all_transactions[df_all_transactions['show_client_side'] == True]
    df_all_transactions = df_all_transactions[df_all_transactions['is_deleted'] == False]
    df_all_transactions = df_all_transactions[df_all_transactions['is_future'] == False]

    # add categories
    df_all_transactions = df_all_transactions.merge(df_categories, on=['category_id'])

    # Export transactions to bubble
    df_all_transactions_export = df_all_transactions
    if df_all_transactions_export.empty is False:
        df_all_transactions_export.rename(
            columns={
                'updated_at': 'bridge_updated_at',
                'name': 'category_name',
                'color': 'category_color',
                'parent_name': 'parent_category_name'
            },
            inplace=True)

        # prepare data before export
        df_all_transactions_export['date'] = df_all_transactions_export['date'].dt.tz_convert('UTC')
        df_all_transactions_export['date'] = df_all_transactions_export['date'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        df_all_transactions_export['item_id'] = item_id
        df_all_transactions_export['amount_in'] = df_all_transactions_export['amount'].apply(amount_in)
        df_all_transactions_export['amount_out'] = df_all_transactions_export['amount'].apply(amount_out)
        df_all_transactions_export['user_uuid'] = user_uuid
        df_all_transactions_export.drop(columns=['bank_description'], inplace=True)
        transactions_count_to_upload = df_all_transactions_export.shape[0]
        df_all_transactions_export = df_all_transactions_export.to_dict('records')

        transactions_output_body = ""
        for transaction in df_all_transactions_export:
            transactions_output_body += json.dumps(transaction) + '\n'
        response_transaction = bulk_export_to_bubble("bridge_transactions", envr=env, body=transactions_output_body)
        transactions_count_success = response_transaction['response'].count('"status":"success"')

    df_transactions_grouped_min_date = df_all_transactions.groupby(['item_id', 'account_id']).agg(
        {'date': 'min'}).reset_index()
    df_transactions_grouped_min_date['date'] = pd.to_datetime(df_transactions_grouped_min_date['date'], utc=True).dt.tz_convert('Europe/Paris')

    # Group by 'item_id', 'account_id', and 'date', and aggregate the sum of 'amount'
    df_transactions_grouped_sum_amount = df_all_transactions.groupby(
        ['item_id', 'account_id', 'date']).agg(
        {'amount': 'sum'}).reset_index()
    df_transactions_grouped_sum_amount['date'] = pd.to_datetime(df_transactions_grouped_sum_amount['date'])

    # Merge grouped_df and grouped_df_min_date on 'account_id', 'item_id'
    merged_df = pd.merge(df_account_grouped_min_date, df_transactions_grouped_min_date,
                         on=['account_id', 'item_id'],
                         suffixes=('_account_min', '_transactions_min'))

    merged_df = merged_df[merged_df['date_account_min'] > merged_df['date_transactions_min']]

    # Initialize an empty dataframe to store the results
    results = pd.DataFrame(
        columns=['account_id', 'item_id', 'date', 'total_daily_amount', 'total_history_amount',
                 'last_balance'])

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
        temp_df['item_id'] = item_id
        temp_df['account_id'] = row['account_id']

        # Merge grouped_df and grouped_df_min_date on 'account_id', 'item_id'
        temp_df = pd.merge(temp_df, df_accounts.drop(columns=['date']),
                           on=['account_id', 'item_id']
                           )

        def history_amount(history_date):
            # Calculate the total history amount per day for the same 'account_id', 'item_id'
            temp_df_amount = df_transactions_grouped_sum_amount[
                (df_transactions_grouped_sum_amount['account_id'] == row['account_id']) &
                (df_transactions_grouped_sum_amount['date'] >= history_date)]
            return temp_df_amount['amount'].sum()

        def daily_amount(history_date):
            # Calculate the total amount per day for the same 'account_id', 'item_id'
            temp_df_amount = df_transactions_grouped_sum_amount[
                (df_transactions_grouped_sum_amount['account_id'] == row['account_id']) &
                (df_transactions_grouped_sum_amount['date'].dt.date == history_date.date())]
            return temp_df_amount['amount'].sum()

        temp_df['total_history_amount'] = temp_df.apply(lambda x: history_amount(x['date']), axis=1)
        temp_df['total_daily_amount'] = temp_df.apply(lambda x: daily_amount(x['date']), axis=1)

        temp_df_balance = df_accounts[
            (df_accounts['account_id'] == row['account_id']) &
            (df_accounts['item_id'] == row['item_id']) &
            (df_accounts['date'] == row['date_account_min'])]

        last_balance = temp_df_balance['balance'].sum()

        temp_df['last_balance'] = last_balance

        # Append the results to the 'results' dataframe
        results = pd.concat([results, temp_df], ignore_index=True)

    results['user_uuid'] = user_uuid
    results['balance'] = (results['last_balance'] - results['total_history_amount']).round(2)
    results['is_start_of_month'] = results['date'].dt.is_month_start
    results['is_end_of_month'] = results['date'].dt.is_month_end
    results['date'] = results['date'].dt.tz_convert('UTC')
    results['start_of_month'] = results['date'].apply(lambda dt: dt.replace(day=1))
    results['end_of_month'] = results['date'] + pd.offsets.MonthEnd(0)

    results.drop(columns=[
        'total_daily_amount', 'total_history_amount', 'last_balance', 'is_paused',
        'loan_details', 'savings_details'
    ], inplace=True)
    results.rename(columns={'account_id': 'id'}, inplace=True)

    if results.empty is False:
        results['date'] = results['date'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        results['start_of_month'] = results['start_of_month'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        results['end_of_month'] = results['end_of_month'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        results[['iban', 'name']] = results[['iban', 'name']].fillna(value='Not available')

        accounts_count_to_upload = results.shape[0]

        results = results.to_dict('records')

        history_output_body = ""
        for result in results:
            history_output_body += json.dumps(result) + '\n'
        response_account = bulk_export_to_bubble("bridge_account", envr=env, body=history_output_body)
        account_count_success = response_account['response'].count('"status":"success"')

    result_script = {
        'transactions_count_to_upload': transactions_count_to_upload,
        'transaction_shape': transactions_count_success,
        'accounts_count_to_upload': accounts_count_to_upload,
        'account_count_success': account_count_success,
        'transaction_account': response_account,
        'transaction_update': response_transaction,
    }
    print(result_script)
    return result_script


@app.route('/trigger_balance_history_calc', methods=['POST'])
@token_required
def trigger_balance_history_calc():
    data = request.json
    user_uuid = data.get('user_uuid')
    bridge_token = data.get('bridge_token')
    item_id = int(data.get('item_id'))
    test = data.get('test')

    result = history_calculation(
        item_id=item_id,
        user_uuid=user_uuid,
        bridge_token=bridge_token,
        test=test
    )

    return jsonify({"result": result})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

    # env = load_credentials(True)
    # result = history_calculation(
    #     user_uuid=env['user_uuid'],
    #     item_id=int(env['item_id']),
    #     bridge_token=env['bridge_auth_token'],
    #     test=True
    # )
    # print(result)
