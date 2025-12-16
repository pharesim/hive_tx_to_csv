import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import OperationalError
import sys

# Set parameters
account_names = ['account1','account2','account3']
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

# Database connection parameters
db_params = {
    'host': 'hafsql-sql.mahdiyari.info',
    'port': 5432,
    'database': 'haf_block_log',
    'user': 'hafsql_public',
    'password': 'hafsql_public'
}

# Intervals to check when transactions need to be split up
intervals = [[timedelta(days=366),'yearly'],
             [timedelta(days=183),'half-yearly'],
             [timedelta(days=31),'monthly'],
             [timedelta(days=7),'weekly'],
             [timedelta(days=1),'daily'], 
             [timedelta(hours=6),'6-hour'], 
             [timedelta(hours=1),'hourly']]

def execute_query(conn, cursor, query, params, tx_type, account_name, filter_account=False):
    results = []
    try:
        cursor.execute(query, params)
        rows = cursor.fetchall()
        for row in rows:
            tx_date, tx_type, direction, sender, recipient, currency, amount = row
            if amount > 0 and (not filter_account or account_name in (sender, recipient)):
                results.append((tx_date, tx_type, direction, sender, recipient, currency, amount))
        return results, True
    except (OperationalError, psycopg2.Error) as e:
        #print(f"\n{e}", end="")
        return [], False

def execute_query_with_intervals(conn, cursor, query, params, tx_type, account_name, start_date, end_date, interval):
    cursor.close()
    conn.close()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    results = []
    current_start = start_date
    filter_account = interval <= timedelta(days=1)
    while current_start < end_date:
        current_end = min(current_start + interval, end_date)
        if filter_account:
            q_parts = query.split("UNION ALL")
            q = []
            for part in q_parts:
                q.append(part.split(" WHERE ")[0] + " WHERE id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)")
            query = " UNION ALL ".join(q)
            if len(params) == 5:
                params = (account_name, current_start, current_end)
            else:
                params = (current_start, current_end) * len(q_parts)
        else:
            params = params[:-2] + (current_start, current_end)
        result, success = execute_query(conn, cursor, query, params, tx_type, account_name, filter_account)
        if not success:
            return [], False, conn, cursor
        results.extend(result)
        current_start = current_end + timedelta(seconds=1)
    return results, True, conn, cursor

def get_transactions_for_account(account_name, start_date, end_date):
    print('Fetching transactions for account ' + account_name + '...')

    # Connect to the hafsql database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # List to hold transaction data
    transactions = []

    # Queries to fetch transactions for each operation type
    queries = [
        ("""
        SELECT hafsql.get_timestamp(id), 'transfer' AS type, 
               CASE 
                   WHEN "to_account" = %s THEN 'incoming' 
                   ELSE 'outgoing' 
               END AS direction, 
               "from_account" AS sender, "to_account" AS recipient, symbol AS currency, amount AS total_amount
        FROM operation_transfer_table
        WHERE ("from_account" = %s OR "to_account" = %s) AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, account_name, account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'interest' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, owner AS recipient, interest_symbol AS currency, interest AS total_amount
        FROM operation_interest_table
        WHERE owner = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'fill_vesting_withdraw' AS type, 
               'unstake' AS direction, 
               CASE 
                   WHEN to_account = from_account THEN 'staked.hive'
                   ELSE from_account
               END AS sender,
               to_account AS recipient, 'HIVE' AS currency, deposited AS total_amount
        FROM operation_fill_vesting_withdraw_table
        WHERE to_account = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'curation_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, curator AS recipient, 'HP' AS currency, hafsql.vests_to_hive(reward,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_curation_reward_table
        WHERE curator = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'fill_convert_request' AS type, 
               'incoming' AS direction, 
               owner AS sender, owner AS recipient, 'HIVE' AS currency, amount_out AS total_amount
        FROM operation_fill_convert_request_table
        WHERE owner = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'convert' AS type, 
               'outgoing' AS direction, 
               owner AS sender, owner AS recipient, 'HBD' AS currency, amount AS total_amount
        FROM operation_convert_table
        WHERE owner = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'comment_benefactor_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, benefactor AS recipient, 'HBD' AS currency, hbd_payout AS total_amount
        FROM operation_comment_benefactor_reward_table
        WHERE benefactor = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        UNION ALL
        SELECT hafsql.get_timestamp(id), 'comment_benefactor_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, benefactor AS recipient, 'HIVE' AS currency, hive_payout AS total_amount
        FROM operation_comment_benefactor_reward_table
        WHERE benefactor = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        UNION ALL
        SELECT hafsql.get_timestamp(id), 'comment_benefactor_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, benefactor AS recipient, 'HP' AS currency, hafsql.vests_to_hive(vesting_payout,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_comment_benefactor_reward_table
        WHERE benefactor = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date, account_name, start_date, end_date, account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'author_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, author AS recipient, 'HBD' AS currency, hbd_payout AS total_amount
        FROM operation_author_reward_table
        WHERE author = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        UNION ALL
        SELECT hafsql.get_timestamp(id), 'author_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, author AS recipient, 'HIVE' AS currency, hive_payout AS total_amount
        FROM operation_author_reward_table
        WHERE author = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        UNION ALL
        SELECT hafsql.get_timestamp(id), 'author_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, author AS recipient, 'HP' AS currency, hafsql.vests_to_hive(vesting_payout,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_author_reward_table
        WHERE author = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date, account_name, start_date, end_date, account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'fill_order' AS type, 
               CASE 
                   WHEN open_owner = %s THEN 'incoming' 
                   ELSE 'outgoing' 
               END AS direction, 
               current_owner AS sender, open_owner AS recipient, current_pays_symbol AS currency, current_pays AS total_amount
        FROM operation_fill_order_table
        WHERE (current_owner = %s OR open_owner = %s) AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, account_name, account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'proposal_pay' AS type, 
               'incoming' AS direction, 
               payer AS sender, receiver AS recipient, 'HBD' AS currency, payment AS total_amount
        FROM operation_proposal_pay_table
        WHERE receiver = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'transfer_to_vesting' AS type, 
               'outgoing' AS direction, 
               "from_account" AS sender, 'staked.hive' AS recipient, 'HIVE' AS currency, amount AS total_amount
        FROM operation_transfer_to_vesting_table
        WHERE "from_account" = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'delegate_vesting_shares' AS type, 
               CASE 
                   WHEN delegatee = %s THEN 'incoming' 
                   ELSE 'outgoing' 
               END AS direction, 
               delegator AS sender, delegatee AS recipient, 'HP' AS currency, hafsql.vests_to_hive(vesting_shares,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_delegate_vesting_shares_table
        WHERE (delegator = %s OR delegatee = %s) AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, account_name, account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'return_vesting_delegation' AS type, 
               'undelegate' AS direction, 
               'delegated.hive' AS sender, account AS recipient, 'HP' AS currency, hafsql.vests_to_hive(vesting_shares,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_return_vesting_delegation_table
        WHERE account = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date)),
        ("""
        SELECT hafsql.get_timestamp(id), 'producer_reward' AS type, 
               'incoming' AS direction, 
               'hive.rewards' AS sender, producer AS recipient, 'HP' AS currency, hafsql.vests_to_hive(vesting_shares,hafd.operation_id_to_block_num(id)) AS total_amount
        FROM operation_producer_reward_table
        WHERE producer = %s AND id BETWEEN hafsql.id_from_timestamp(%s) AND hafsql.id_from_timestamp(%s)
        """, (account_name, start_date, end_date))
    ]

    for query, params in queries:
        tx_type = query.split(',')[1].split("AS")[0].strip()
        result, success = execute_query(conn, cursor, query, params, tx_type, account_name)
        if not success:
            for interval, interval_name in intervals:
                if interval < end_date - start_date:
                    print(f"\nFailed getting {tx_type} transactions. Trying {interval_name} intervals...", end="")
                    result, success, conn, cursor = execute_query_with_intervals(conn, cursor, query, params, tx_type, account_name, start_date, end_date, interval)
                    if success:
                        break
            if not success:
                print(f"\nFailed getting {tx_type} transactions even with these low intervals. Giving up.")
                sys.exit(1)
        transactions.extend(result)
        print(f"\n{tx_type} transactions collected: {len(result)}", end="")

    cursor.close()
    conn.close()

    return transactions

def aggregate_transactions(transactions):
    df = pd.DataFrame(transactions, columns=['date', 'type', 'direction', 'sender', 'recipient', 'currency', 'amount'])
    df = df.groupby(['date', 'type', 'direction', 'sender', 'recipient', 'currency']).sum().reset_index()
    return df

end_date = end_date + timedelta(days=1) - timedelta(seconds=1)
for a in account_names:
    # Get transactions for the given account and time range
    transactions = get_transactions_for_account(a, start_date, end_date)

    print(f"\nTotal transactions collected: {len(transactions)}")

    # Sort transactions by date
    transactions.sort(key=lambda x: x[0])

    # Aggregate the transactions by date and type
    aggregated_data = aggregate_transactions(transactions)

    # Export to CSV
    csv_filename = f"{a}_transactions_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}_hafsql.csv"
    aggregated_data.to_csv(csv_filename, index=False)

    print(f"CSV file saved as: {csv_filename}")
