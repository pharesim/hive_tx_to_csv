from beem import Hive
from beem.account import Account
import pandas as pd
from datetime import datetime
import requests
import json

# Set parameters
account_names = ['account1','account2']
start_date = datetime(2023, 12, 1)
end_date = datetime(2024, 1, 1)

# Initialize the Hive blockchain instance
hive = Hive(node=['https://api.hive.blog','https://api.deathwing.me'])
hafsql = 'https://rpc.mahdiyari.info'

def get_vests_to_hive_ratio(block_num):
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = '{"jsonrpc":"2.0", "method":"hafsql.dynamic_global_properties", "params":{"block_num": '+str(block_num)+'}, "id":1}'
    response = 0
    while response == 0:
        try:
            response = requests.post(hafsql, headers=headers, data=data)
            global_properties = json.loads(response.text)['result'][0]
        except:
            response = 0
    
    # Calculate VESTS to HIVE ratio
    total_vesting_fund_hive = float(global_properties['total_vesting_fund_hive'])
    total_vesting_shares = float(global_properties['total_vesting_shares'])
    
    return total_vesting_fund_hive / total_vesting_shares

def get_transactions_for_account(account_name, start_date, end_date):
    print('Scanning transactions for account '+account_name+'...')

    account = Account(account_name, blockchain_instance=hive)
    
    # List to hold transaction data
    transactions = []

   # Operations to include/exclude (not working with history_reverse, needs to be filtered in loop)
    ops = ['return_vesting_delegation','delegate_vesting_shares','transfer_to_vesting','proposal_pay','fill_order','author_reward','comment_benefactor_reward','convert','fill_convert_request','producer_reward','curation_reward','fill_vesting_withdraw','transfer']
    excluded = ['proxy_cleared','create_claimed_account','account_created','witness_update','account_update','witness_set_properties','vote','effective_comment_vote','account_witness_vote','comment','claim_reward_balance','update_proposal_votes','custom_json','comment_reward','comment_payout_update','comment_options','withdraw_vesting','delayed_voting','limit_order_create','limit_order_cancelled']
       
    # Iterate over account history
    scanned_tx = 0
    last_ratio_update = 0
    for h in account.history_reverse(stop=start_date,start=end_date):
        scanned_tx = scanned_tx + 1
        if scanned_tx%100 == 0:
            print('scanned '+str(scanned_tx)+' transactions ('+h['timestamp']+')')

        timestamp = h['timestamp'].replace('T', ' ')
        tx_time = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

        # Get block number from transaction
        block_num = h['block']

        # Filter monetary transactions and witness rewards
        if h['type'] not in excluded:

            # Get VESTS to HIVE ratio at the time of the transaction
            if last_ratio_update != block_num:            
                vests_to_hive_ratio = get_vests_to_hive_ratio(block_num)
                last_ratio_update = block_num
            
            # Determine the currency and amount
            currency = None
            amount = 0
            sender = None
            recipient = None
            direction = None
            
            # Transfer
            if h['type'] == 'transfer':
                if h['amount']['nai'] == '@@000000021':
                    currency = 'HIVE'
                else:
                    currency = 'HBD'
                amount = int(h['amount']['amount']) / 10**h['amount']['precision']
                sender = h['from']
                recipient = h['to']
                direction = 'incoming' if recipient == account_name else 'outgoing'

            # Fill vesting withdraw
            elif h['type'] == 'fill_vesting_withdraw':
                currency = 'HIVE'
                amount = int(h['deposited']['amount']) / 1000
                sender = 'staked.hive'
                recipient = h['to_account']
                direction = 'unstake'
            
            # Curation Reward
            elif h['type'] == 'curation_reward':
                vesting_amount = float(h['reward']['amount'])
                hive_amount = round(vesting_amount * vests_to_hive_ratio / 1000,3)
                currency = 'HP'
                amount = hive_amount
                sender = 'hive.rewards'
                recipient = account_name
                direction = 'incoming'

            # Witness Producer Reward
            elif h['type'] == 'producer_reward':
                vesting_amount = float(h['vesting_shares']['amount'])
                hive_amount = round(vesting_amount * vests_to_hive_ratio / 1000,3)
                currency = 'HP'
                amount = hive_amount
                sender = 'hive.rewards'
                recipient = account_name
                direction = 'incoming'

            # Fill convert request
            elif h['type'] == 'fill_convert_request':
                currency = 'HIVE'
                amount = int(h['amount_out']['amount']) / 1000
                sender = h['owner']
                recipient = h['account']
                direction = 'incoming'

            # Convert request
            elif h['type'] == 'convert':
                currency = 'HBD'
                amount = int(h['amount']['amount']) / 1000
                sender = h['owner']
                recipient = h['account']
                direction = 'outgoing'

            # Comment benefactor reward
            elif h['type'] == 'comment_benefactor_reward':
                if int(h['hbd_payout']['amount']) > 0:
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['benefactor'], 'HBD', int(h['hbd_payout']['amount'])/1000))
                if int(h['hive_payout']['amount']) > 0:
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['benefactor'], 'HIVE', int(h['hive_payout']['amount'])/1000))
                if float(h['vesting_payout']['amount']) > 0:
                    hp_amount = round(int(h['vesting_payout']['amount']) * vests_to_hive_ratio / 1000,3)
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['benefactor'], 'HP', hp_amount))
                continue
            
            # Author Reward
            elif h['type'] == 'author_reward':
                if int(h['hbd_payout']['amount']) > 0:
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['author'], 'HBD', int(h['hbd_payout']['amount'])/1000))
                if int(h['hive_payout']['amount']) > 0:
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['author'], 'HIVE', int(h['hive_payout']['amount'])/1000))
                if float(h['vesting_payout']['amount']) > 0:
                    hp_amount = round(int(h['vesting_payout']['amount']) * vests_to_hive_ratio / 1000,3)
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.rewards', h['author'], 'HP', hp_amount))
                continue

            # Fill order
            elif h['type'] == 'fill_order':
                if h['current_pays']['nai']  == '@@000000021':
                    current_currency = 'HIVE'
                    open_currency = 'HBD'
                elif h['current_pays']['nai'] == '@@000000013':
                    current_currency = 'HBD'
                    open_currency = 'HIVE'
                if h['current_owner'] == account_name:
                    transactions.append((tx_time.date(), h['type'], 'outgoing', h['current_owner'], 'hive.market', current_currency, int(h['current_pays']['amount'])/1000))
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.market', h['current_owner'], open_currency, int(h['open_pays']['amount'])/1000))
                elif h['open_owner'] == account_name:
                    transactions.append((tx_time.date(), h['type'], 'incoming', 'hive.market', h['open_owner'], current_currency, int(h['current_pays']['amount'])/1000))
                    transactions.append((tx_time.date(), h['type'], 'outgoing', h['open_owner'], 'hive.market', open_currency, int(h['open_pays']['amount'])/1000))
                continue

            # Proposal pay
            elif h['type'] == 'proposal_pay':
                currency = 'HBD'
                amount = int(h['payment']['amount']) / 1000
                sender = h['payer']
                recipient = h['receiver']
                direction = 'incoming'

            # Power up
            elif h['type'] == 'transfer_to_vesting':
                currency = 'HIVE'
                amount = int(h['amount']['amount']) / 1000
                sender = h['from']
                recipient = 'staked.hive'
                direction = 'stake'

            # Delegate HP
            elif h['type'] == 'delegate_vesting_shares':
                currency = 'HP'
                vesting_amount = float(h['vesting_shares']['amount'])
                amount = round(vesting_amount * vests_to_hive_ratio / 1000,3)
                sender = h['delegator']
                recipient = h['delegatee']
                direction = 'delegate'

            # Undelegate HP
            elif h['type'] == 'return_vesting_delegation':
                currency = 'HP'
                vesting_amount = float(h['vesting_shares']['amount'])
                amount = round(vesting_amount * vests_to_hive_ratio / 1000,3)
                sender = 'delegated.hive'
                recipient = h['account']
                direction = 'undelegate'

            else:
                print(h)
            
            # Append data as (date, type, direction, sender, recipient, currency, amount)
            if currency and amount > 0:
                transactions.append((tx_time.date(), h['type'], direction, sender, recipient, currency, amount))
    
    return transactions

def aggregate_transactions(transactions):
    # Create DataFrame
    df = pd.DataFrame(transactions, columns=['date', 'type', 'direction', 'sender', 'recipient', 'currency', 'amount'])
    
    # Aggregate by date, type, direction, currency, sender, and recipient
    aggregated_df = df.groupby(['date', 'type', 'direction', 'sender', 'recipient', 'currency']).sum().reset_index()
    
    return aggregated_df


for a in account_names:

	# Get transactions for the given account and time range
	transactions = get_transactions_for_account(a, start_date, end_date)

	# Aggregate the transactions by date and type
	aggregated_data = aggregate_transactions(transactions)

	# Export to CSV
	csv_filename = f"{a}_transactions_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.csv"
	aggregated_data.to_csv(csv_filename, index=False)

	print(f"CSV file saved as: {csv_filename}")

