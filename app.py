import json
import pandas as pd
from utilities import load_credentials, bulk_export_to_bubble, \
    get_data_from_bridge_api_list_accounts, get_data_from_bridge_api_list_transactions_by_account, amount_in, \
    amount_out, get_object_from_bubble, token_required
from flask import Flask, request, jsonify
from dateutil.relativedelta import relativedelta
import datetime
import pytz

app = Flask(__name__)


def history_calculation(item_id, user_uuid, bridge_token, test):
    # Load env variables
    env = load_credentials(test)

    # Load categories and transform
    df_categories = get_object_from_bubble("bridge_categories", envr=env)
    df_categories = df_categories[['id', 'name', 'color', 'parent_name']]
    df_categories.rename(columns={'id': 'category_id'}, inplace=True)

    # Load banks and transform
    df_banks = get_object_from_bubble("bridge_bank", envr=env)
    df_banks = df_banks[['id', 'name']]
    df_banks.rename(
        columns={
            'id': 'bank_id',
            'name': 'bank_name'
        },
        inplace=True
    )

    # Fetch account data and transform
    df_accounts = get_data_from_bridge_api_list_accounts(
        env['bridge_client_id'],
        env['bridge_client_secret'],
        access_token=bridge_token,
        item_id=item_id
    )
    account_types_to_keep = ['checking', 'savings', 'card', 'shared_saving_plan', 'brokerage']
    df_accounts = df_accounts[df_accounts['type'].isin(account_types_to_keep)]
    df_accounts = df_accounts[df_accounts['currency_code'] == "EUR"]
    list_of_account_id = df_accounts['id'].tolist()
    list_of_item_id = df_accounts['item_id'].tolist()
    list_of_account_id.append(0)
    list_of_item_id.append(0)

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
    df_all_transactions = df_all_transactions[df_all_transactions['account_id'].isin(list_of_account_id)]

    # add context
    df_all_transactions = df_all_transactions.merge(df_categories, on=['category_id'])
    df_all_transactions['amount_in'] = df_all_transactions['amount'].apply(amount_in)
    df_all_transactions['amount_out'] = df_all_transactions['amount'].apply(amount_out)
    df_all_transactions['user_uuid'] = user_uuid

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

        # prepare data for bubble formatting
        format_df_all_transactions_daily_cat = df_all_transactions_export.groupby(
            ['date', 'account_id', 'item_id']).agg(
            amount_in=('amount_in', 'sum'),
            amount_out=('amount_out', 'sum')
        ).reset_index()
        format_df_all_transactions_daily_cat['date'] = format_df_all_transactions_daily_cat['date'].dt.tz_convert('UTC')

        format_df_all_transactions_daily_cat = df_all_transactions_export.groupby(
            ['date', 'account_id', 'item_id']).agg(
            amount_in=('amount_in', 'sum'),
            amount_out=('amount_out', 'sum')
        ).reset_index()
        format_df_all_transactions_daily_cat['date'] = format_df_all_transactions_daily_cat['date'].dt.tz_convert('UTC')

        # prepare data before export
        df_all_transactions_export['date'] = df_all_transactions_export['date'].dt.tz_convert('UTC')
        df_all_transactions_export['date'] = df_all_transactions_export['date'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

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
    df_transactions_grouped_min_date['date'] = pd.to_datetime(df_transactions_grouped_min_date['date'],
                                                              utc=True).dt.tz_convert('Europe/Paris')

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
        start_date = row['date_transactions_min']
        end_date = row['date_account_min']

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

    # add banks
    results = results.merge(df_banks, on=['bank_id'])

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

    if results.empty is False:
        format_result = results.merge(format_df_all_transactions_daily_cat, how='left',
                                      on=['date', 'account_id', 'item_id'])
        format_result.fillna(0, inplace=True)

        df_formatted = pd.DataFrame()

        # Get current date
        current_date = datetime.datetime.now(pytz.timezone("Europe/Paris"))

        for account_id, item_id in zip(list_of_account_id, list_of_item_id):
            if account_id == 0:
                temp_format_results = format_result
            else:
                temp_format_results = format_result[format_result['account_id'] == account_id]
                temp_format_results = temp_format_results[temp_format_results['item_id'] == item_id]

            # 7 days calculations
            earliest_date = current_date - relativedelta(days=7)
            temp_format_results_filtered = temp_format_results[temp_format_results['date'] >= earliest_date]
            temp_format_results_filtered = temp_format_results_filtered.sort_values('date')

            seven_d_dates = ", ".join(map(str, temp_format_results_filtered['date'].dt.strftime('%d/%m/%Y').tolist()))
            seven_d_balance = ", ".join(map(str, temp_format_results_filtered['balance'].tolist()))
            seven_d_transactions_in = ", ".join(map(str, temp_format_results_filtered['amount_in'].tolist()))
            seven_d_transactions_out = ", ".join(map(str, temp_format_results_filtered['amount_out'].tolist()))

            if account_id == 0:
                temp_cat = df_all_transactions
            else:
                temp_cat = df_all_transactions[
                    (df_all_transactions['account_id'] == account_id) & (df_all_transactions['item_id'] == item_id)]

            temp_cat['date'] = pd.to_datetime(temp_cat['date'])
            temp_cat = temp_cat[temp_cat['date'] >= earliest_date]

            temp_cat = temp_cat.groupby(by=['category_color', 'category_name']).agg(
                amount_in=('amount_in', 'sum'),
                amount_out=('amount_out', 'sum')
            ).reset_index()

            seven_d_cat_amount_in = ", ".join(map(str, temp_cat['amount_in'].tolist()))
            seven_d_cat_amount_out = ", ".join(map(str, temp_cat['amount_out'].tolist()))
            seven_d_cat_color = ",".join(map(str, temp_cat['category_color'].tolist()))
            seven_d_cat_name = ", ".join(map(str, temp_cat['category_name'].tolist()))

            # 30 days calculations
            earliest_date = current_date - relativedelta(days=30)
            temp_format_results_filtered = temp_format_results[temp_format_results['date'] >= earliest_date]
            temp_format_results_filtered = temp_format_results_filtered.sort_values('date')

            thirty_d_dates = ", ".join(map(str, temp_format_results_filtered['date'].dt.strftime('%d/%m/%Y').tolist()))
            thirty_d_balance = ", ".join(map(str, temp_format_results_filtered['balance'].tolist()))
            thirty_d_transactions_in = ", ".join(map(str, temp_format_results_filtered['amount_in'].tolist()))
            thirty_d_transactions_out = ", ".join(map(str, temp_format_results_filtered['amount_out'].tolist()))

            if account_id == 0:
                temp_cat = df_all_transactions
            else:
                temp_cat = df_all_transactions[
                    (df_all_transactions['account_id'] == account_id) & (df_all_transactions['item_id'] == item_id)]

            temp_cat['date'] = pd.to_datetime(temp_cat['date'])
            temp_cat = temp_cat[temp_cat['date'] >= earliest_date]

            temp_cat = temp_cat.groupby(by=['category_color', 'category_name']).agg(
                amount_in=('amount_in', 'sum'),
                amount_out=('amount_out', 'sum')
            ).reset_index()

            thirty_d_cat_amount_in = ", ".join(map(str, temp_cat['amount_in'].tolist()))
            thirty_d_cat_amount_out = ", ".join(map(str, temp_cat['amount_out'].tolist()))
            thirty_d_cat_color = ",".join(map(str, temp_cat['category_color'].tolist()))
            thirty_d_cat_name = ", ".join(map(str, temp_cat['category_name'].tolist()))

            # 3 months calculations
            earliest_date = (current_date - relativedelta(months=2)).replace(day=1)

            temp_format_results_filtered = temp_format_results[temp_format_results['date'] >= earliest_date]
            # assuming df is your DataFrame and 'dates' is your datetime column
            temp_format_results_filtered['month_year_date'] = temp_format_results_filtered['date'].dt.strftime('%b %Y')

            # Since you want the month names in French, you can use a dictionary to map them
            months_en_to_fr = {
                'Jan': 'Janv',
                'Feb': 'Févr',
                'Mar': 'Mars',
                'Apr': 'Avr',
                'May': 'Mai',
                'Jun': 'Juin',
                'Jul': 'Juil',
                'Aug': 'Août',
                'Sep': 'Sept',
                'Oct': 'Oct',
                'Nov': 'Nov',
                'Dec': 'Déc'
            }

            # Replace English month abbreviations with French ones
            for en, fr in months_en_to_fr.items():
                temp_format_results_filtered['month_year_date'] = temp_format_results_filtered[
                    'month_year_date'].str.replace(en, fr)

            # Group by 'month_year', 'item_id', 'id' and get first() and last() 'balance'
            if account_id == 0:
                temp_format_results_filtered = temp_format_results_filtered.groupby(
                    ['date', 'start_of_month', 'month_year_date']).agg(
                    balance=('balance', 'sum'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()
                temp_format_results_filtered = temp_format_results_filtered.sort_values('start_of_month')
                temp_agg_results = temp_format_results_filtered.groupby(['month_year_date']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()

            else:
                temp_agg_results = temp_format_results_filtered.groupby(
                    ['month_year_date', 'item_id', 'account_id']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()

            # Sort by date
            temp_agg_results = temp_agg_results.sort_values('start_of_month')

            three_m_dates = ", ".join(map(str, temp_agg_results['month_year_date'].tolist()))

            three_m_balance_som = ", ".join(map(str, temp_agg_results['balance_start_of_month'].tolist()))
            three_m_balance_eom = ", ".join(map(str, temp_agg_results['balance_end_of_month'].tolist()))
            three_m_transactions_in = ", ".join(map(str, temp_agg_results['amount_in'].tolist()))
            three_m_transactions_out = ", ".join(map(str, temp_agg_results['amount_out'].tolist()))

            if account_id == 0:
                temp_cat = df_all_transactions
            else:
                temp_cat = df_all_transactions[
                    (df_all_transactions['account_id'] == account_id) & (df_all_transactions['item_id'] == item_id)]

            temp_cat['date'] = pd.to_datetime(temp_cat['date'])
            temp_cat = temp_cat[temp_cat['date'] >= earliest_date]

            temp_cat = temp_cat.groupby(by=['category_color', 'category_name']).agg(
                amount_in=('amount_in', 'sum'),
                amount_out=('amount_out', 'sum')
            ).reset_index()

            three_m_cat_amount_in = ", ".join(map(str, temp_cat['amount_in'].tolist()))
            three_m_cat_amount_out = ", ".join(map(str, temp_cat['amount_out'].tolist()))
            three_m_cat_color = ",".join(map(str, temp_cat['category_color'].tolist()))
            three_m_cat_name = ", ".join(map(str, temp_cat['category_name'].tolist()))

            # 6 months calculations
            earliest_date = (current_date - relativedelta(months=5)).replace(day=1)

            temp_format_results_filtered = temp_format_results[temp_format_results['date'] >= earliest_date]
            # assuming df is your DataFrame and 'dates' is your datetime column
            temp_format_results_filtered['month_year_date'] = temp_format_results_filtered['date'].dt.strftime('%b %Y')

            # Since you want the month names in French, you can use a dictionary to map them
            months_en_to_fr = {
                'Jan': 'Janv',
                'Feb': 'Févr',
                'Mar': 'Mars',
                'Apr': 'Avr',
                'May': 'Mai',
                'Jun': 'Juin',
                'Jul': 'Juil',
                'Aug': 'Août',
                'Sep': 'Sept',
                'Oct': 'Oct',
                'Nov': 'Nov',
                'Dec': 'Déc'
            }

            # Replace English month abbreviations with French ones
            for en, fr in months_en_to_fr.items():
                temp_format_results_filtered['month_year_date'] = temp_format_results_filtered[
                    'month_year_date'].str.replace(en, fr)

            # Sort by date
            temp_format_results_filtered = temp_format_results_filtered.sort_values('date')

            # Group by 'month_year', 'item_id', 'id' and get first() and last() 'balance'
            if account_id == 0:
                temp_format_results_filtered = temp_format_results_filtered.groupby(
                    ['date', 'start_of_month', 'month_year_date']).agg(
                    balance=('balance', 'sum'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()
                temp_format_results_filtered = temp_format_results_filtered.sort_values('start_of_month')
                temp_agg_results = temp_format_results_filtered.groupby(['month_year_date']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()
            else:
                temp_agg_results = temp_format_results_filtered.groupby(
                    ['month_year_date', 'item_id', 'account_id']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()

            # Sort by date
            temp_agg_results = temp_agg_results.sort_values('start_of_month')

            six_m_dates = ", ".join(map(str, temp_agg_results['month_year_date'].tolist()))

            six_m_balance_som = ", ".join(map(str, temp_agg_results['balance_start_of_month'].tolist()))
            six_m_balance_eom = ", ".join(map(str, temp_agg_results['balance_end_of_month'].tolist()))
            six_m_transactions_in = ", ".join(map(str, temp_agg_results['amount_in'].tolist()))
            six_m_transactions_out = ", ".join(map(str, temp_agg_results['amount_out'].tolist()))

            if account_id == 0:
                temp_cat = df_all_transactions
            else:
                temp_cat = df_all_transactions[
                    (df_all_transactions['account_id'] == account_id) & (df_all_transactions['item_id'] == item_id)]

            temp_cat['date'] = pd.to_datetime(temp_cat['date'])
            temp_cat = temp_cat[temp_cat['date'] >= earliest_date]

            temp_cat = temp_cat.groupby(by=['category_color', 'category_name']).agg(
                amount_in=('amount_in', 'sum'),
                amount_out=('amount_out', 'sum')
            ).reset_index()

            six_m_cat_amount_in = ", ".join(map(str, temp_cat['amount_in'].tolist()))
            six_m_cat_amount_out = ", ".join(map(str, temp_cat['amount_out'].tolist()))
            six_m_cat_color = ",".join(map(str, temp_cat['category_color'].tolist()))
            six_m_cat_name = ", ".join(map(str, temp_cat['category_name'].tolist()))

            # 12 months calculations
            earliest_date = (current_date - relativedelta(months=11)).replace(day=1)

            temp_format_results_filtered = temp_format_results[temp_format_results['date'] >= earliest_date]
            # assuming df is your DataFrame and 'dates' is your datetime column
            temp_format_results_filtered['month_year_date'] = temp_format_results_filtered['date'].dt.strftime('%b %Y')

            # Since you want the month names in French, you can use a dictionary to map them
            months_en_to_fr = {
                'Jan': 'Janv',
                'Feb': 'Févr',
                'Mar': 'Mars',
                'Apr': 'Avr',
                'May': 'Mai',
                'Jun': 'Juin',
                'Jul': 'Juil',
                'Aug': 'Août',
                'Sep': 'Sept',
                'Oct': 'Oct',
                'Nov': 'Nov',
                'Dec': 'Déc'
            }

            # Replace English month abbreviations with French ones
            for en, fr in months_en_to_fr.items():
                temp_format_results_filtered['month_year_date'] = temp_format_results_filtered[
                    'month_year_date'].str.replace(en, fr)

            # Sort by date
            temp_format_results_filtered = temp_format_results_filtered.sort_values('date')

            # Group by 'month_year', 'item_id', 'id' and get first() and last() 'balance'
            if account_id == 0:
                temp_format_results_filtered = temp_format_results_filtered.groupby(
                    ['date', 'start_of_month', 'month_year_date']).agg(
                    balance=('balance', 'sum'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()
                temp_format_results_filtered = temp_format_results_filtered.sort_values('start_of_month')
                temp_agg_results = temp_format_results_filtered.groupby(['month_year_date']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()
                # temp_agg_results['account_id'] = None
                # temp_agg_results['item_id'] = None
            else:
                temp_agg_results = temp_format_results_filtered.groupby(
                    ['month_year_date', 'item_id', 'account_id']).agg(
                    balance_start_of_month=('balance', 'first'),
                    balance_end_of_month=('balance', 'last'),
                    start_of_month=('start_of_month', 'first'),
                    amount_in=('amount_in', 'sum'),
                    amount_out=('amount_out', 'sum'),
                ).reset_index()

            # Sort by date
            temp_agg_results = temp_agg_results.sort_values('start_of_month')

            twelve_m_dates = ", ".join(map(str, temp_agg_results['month_year_date'].tolist()))

            twelve_m_balance_som = ", ".join(map(str, temp_agg_results['balance_start_of_month'].tolist()))
            twelve_m_balance_eom = ", ".join(map(str, temp_agg_results['balance_end_of_month'].tolist()))
            twelve_m_transactions_in = ", ".join(map(str, temp_agg_results['amount_in'].tolist()))
            twelve_m_transactions_out = ", ".join(map(str, temp_agg_results['amount_out'].tolist()))

            if account_id == 0:
                temp_cat = df_all_transactions
            else:
                temp_cat = df_all_transactions[
                    (df_all_transactions['account_id'] == account_id) & (df_all_transactions['item_id'] == item_id)]

            temp_cat['date'] = pd.to_datetime(temp_cat['date'])
            temp_cat = temp_cat[temp_cat['date'] >= earliest_date]

            temp_cat = temp_cat.groupby(by=['category_color', 'category_name']).agg(
                amount_in=('amount_in', 'sum'),
                amount_out=('amount_out', 'sum')
            ).reset_index()

            twelve_m_cat_amount_in = ", ".join(map(str, temp_cat['amount_in'].tolist()))
            twelve_m_cat_amount_out = ", ".join(map(str, temp_cat['amount_out'].tolist()))
            twelve_m_cat_color = ",".join(map(str, temp_cat['category_color'].tolist()))
            twelve_m_cat_name = ", ".join(map(str, temp_cat['category_name'].tolist()))

            new_row = {
                "12m_balance_eom": twelve_m_balance_eom,
                "12m_balance_som": twelve_m_balance_som,
                "12m_cat_amount_in": twelve_m_cat_amount_in,
                "12m_cat_amount_out": twelve_m_cat_amount_out,
                "12m_cat_color": twelve_m_cat_color,
                "12m_cat_name": twelve_m_cat_name,
                "12m_dates": twelve_m_dates,
                "12m_transactions_in": twelve_m_transactions_in,
                "12m_transactions_out": twelve_m_transactions_out,
                "30d_balance": thirty_d_balance,
                "30d_cat_amount_in": thirty_d_cat_amount_in,
                "30d_cat_amount_out": thirty_d_cat_amount_out,
                "30d_cat_color": thirty_d_cat_color,
                "30d_cat_name": thirty_d_cat_name,
                "30d_dates": thirty_d_dates,
                "30d_transactions_in": thirty_d_transactions_in,
                "30d_transactions_out": thirty_d_transactions_out,
                "3m_balance_eom": three_m_balance_eom,
                "3m_balance_som": three_m_balance_som,
                "3m_cat_amount_in": three_m_cat_amount_in,
                "3m_cat_amount_out": three_m_cat_amount_out,
                "3m_cat_color": three_m_cat_color,
                "3m_cat_name": three_m_cat_name,
                "3m_dates": three_m_dates,
                "3m_transactions_in": three_m_transactions_in,
                "3m_transactions_out": three_m_transactions_out,
                "6m_balance_eom": six_m_balance_eom,
                "6m_balance_som": six_m_balance_som,
                "6m_cat_amount_in": six_m_cat_amount_in,
                "6m_cat_amount_out": six_m_cat_amount_out,
                "6m_cat_color": six_m_cat_color,
                "6m_cat_name": six_m_cat_name,
                "6m_dates": six_m_dates,
                "6m_transactions_in": six_m_transactions_in,
                "6m_transactions_out": six_m_transactions_out,
                "7d_balance": seven_d_balance,
                "7d_cat_amount_in": seven_d_cat_amount_in,
                "7d_cat_amount_out": seven_d_cat_amount_out,
                "7d_cat_color": seven_d_cat_color,
                "7d_cat_name": seven_d_cat_name,
                "7d_dates": seven_d_dates,
                "7d_transactions_in": seven_d_transactions_in,
                "7d_transactions_out": seven_d_transactions_out,
                "account_id": account_id,
                "item_id": item_id,
                "user_uuid": user_uuid
            }

            temp_df = pd.DataFrame(new_row, index=[0])
            df_formatted = pd.concat([df_formatted, temp_df], ignore_index=True)
        df_formatted = df_formatted.to_dict('records')

        formatted_output_body = ""
        for formatted in df_formatted:
            formatted_output_body += json.dumps(formatted) + '\n'
        response_formated = bulk_export_to_bubble("bridge_account_history_formatted", envr=env,
                                                  body=formatted_output_body)

        results.rename(columns={'account_id': 'id'}, inplace=True)
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
        'account_count_success': account_count_success
    }
    return result_script


@app.route('/trigger_balance_history_calc', methods=['POST'])
@token_required
def trigger_balance_history_calc():
    data = request.json
    user_uuid = data.get('user_uuid')
    bridge_token = data.get('bridge_token')
    item_id = int(data.get('item_id'))
    test = data.get('test')

    if (test == 'non') or (test == 'no'):
        test = False
    else:
        test = True

    print("Start of calculation for ", item_id, ", test is ", test)

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
    # history_calculation(
    #     user_uuid=env['user_uuid'],
    #     item_id=int(env['item_id']),
    #     bridge_token=env['bridge_auth_token'],
    #     test=True
    # )
