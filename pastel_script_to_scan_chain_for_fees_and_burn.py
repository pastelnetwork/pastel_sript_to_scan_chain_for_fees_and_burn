import logging
import asyncio
import json
import decimal
import base64
import os
from logging.handlers import RotatingFileHandler
from io import StringIO
from pathlib import Path
import urllib.parse as urlparse
import pandas as pd
from tqdm import tqdm
from httpx import AsyncClient, Limits, Timeout
    
# Setup logging:
log = logging.getLogger("PastelExplorerBurnAndFeeTracker")
log.propagate = False  # Stop propagating to parent loggers
for handler in log.handlers[:]:
    log.removeHandler(handler)
log.setLevel(logging.INFO)
log_folder = Path('old_application_log_files')
log_folder.mkdir(parents=True, exist_ok=True)
log_file_full_path = log_folder / 'pastel_explorer_burn_and_fee_tracker_log.txt'
rotating_handler = RotatingFileHandler(log_file_full_path, maxBytes=5*1024*1024, backupCount=10)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
rotating_handler.setFormatter(formatter)
log.addHandler(rotating_handler)
log.addHandler(ch)


def get_local_rpc_settings_func(directory_with_pastel_conf=os.path.expanduser("~/.pastel/")):
    with open(os.path.join(directory_with_pastel_conf, "pastel.conf"), 'r') as f:
        lines = f.readlines()
    other_flags = {}
    rpchost = '127.0.0.1'
    rpcport = '9932'
    for line in lines:
        if line.startswith('rpcport'):
            value = line.split('=')[1]
            rpcport = value.strip()
        elif line.startswith('rpcuser'):
            value = line.split('=')[1]
            rpcuser = value.strip()
        elif line.startswith('rpcpassword'):
            value = line.split('=')[1]
            rpcpassword = value.strip()
        elif line.startswith('rpchost'):
            pass
        elif line == '\n':
            pass
        else:
            current_flag = line.strip().split('=')[0].strip()
            current_value = line.strip().split('=')[1].strip()
            other_flags[current_flag] = current_value
    return rpchost, rpcport, rpcuser, rpcpassword, other_flags
    

class JSONRPCException(Exception):
    def __init__(self, rpc_error):
        parent_args = []
        try:
            parent_args.append(rpc_error['message'])
        except Exception as e:
            log.error(f"Error occurred in JSONRPCException: {e}")
            pass
        Exception.__init__(self, *parent_args)
        self.error = rpc_error
        self.code = rpc_error['code'] if 'code' in rpc_error else None
        self.message = rpc_error['message'] if 'message' in rpc_error else None

    def __str__(self):
        return '%d: %s' % (self.code, self.message)

    def __repr__(self):
        return '<%s \'%s\'>' % (self.__class__.__name__, self)

def EncodeDecimal(o):
    if isinstance(o, decimal.Decimal):
        return float(round(o, 8))
    raise TypeError(repr(o) + " is not JSON serializable")
    
class AsyncAuthServiceProxy:
    max_concurrent_requests = 5000
    _semaphore = asyncio.BoundedSemaphore(max_concurrent_requests)
    def __init__(self, service_url, service_name=None, reconnect_timeout=15, reconnect_amount=2, request_timeout=20):
        self.service_url = service_url
        self.service_name = service_name
        self.url = urlparse.urlparse(service_url)        
        self.client = AsyncClient(timeout=Timeout(request_timeout), limits=Limits(max_connections=200, max_keepalive_connections=10))
        self.id_count = 0
        user = self.url.username
        password = self.url.password
        authpair = f"{user}:{password}".encode('utf-8')
        self.auth_header = b'Basic ' + base64.b64encode(authpair)
        self.reconnect_timeout = reconnect_timeout
        self.reconnect_amount = reconnect_amount
        self.request_timeout = request_timeout

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError
        if self.service_name is not None:
            name = f"{self.service_name}.{name}"
        return AsyncAuthServiceProxy(self.service_url, name)

    async def __call__(self, *args):
        async with self._semaphore: # Acquire a semaphore
            self.id_count += 1
            postdata = json.dumps({
                'version': '1.1',
                'method': self.service_name,
                'params': args,
                'id': self.id_count
            }, default=EncodeDecimal)
            headers = {
                'Host': self.url.hostname,
                'User-Agent': "AuthServiceProxy/0.1",
                'Authorization': self.auth_header,
                'Content-type': 'application/json'
            }
            for i in range(self.reconnect_amount):
                try:
                    if i > 0:
                        log.warning(f"Reconnect try #{i+1}")
                        sleep_time = self.reconnect_timeout * (2 ** i)
                        log.info(f"Waiting for {sleep_time} seconds before retrying.")
                        await asyncio.sleep(sleep_time)
                    response = await self.client.post(
                        self.service_url, headers=headers, data=postdata)
                    break
                except Exception as e:
                    log.error(f"Error occurred in __call__: {e}")
                    err_msg = f"Failed to connect to {self.url.hostname}:{self.url.port}"
                    rtm = self.reconnect_timeout
                    if rtm:
                        err_msg += f". Waiting {rtm} seconds."
                    log.exception(err_msg)
            else:
                log.error("Reconnect tries exceeded.")
                return
            response_json = response.json()
            if response_json['error'] is not None:
                raise JSONRPCException(response_json['error'])
            elif 'result' not in response_json:
                raise JSONRPCException({
                    'code': -343, 'message': 'missing JSON-RPC result'})
            else:
                return response_json['result']
                
async def main():
    rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
    rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")

    async def get_all_pastel_blockchain_tickets_func(verbose=0):
        if verbose:
            log.info('Now retrieving all Pastel blockchain tickets...')
        tickets_obj = {}
        fee_paying_tickets_obj = {}
        list_of_ticket_types = ['id', 'nft', 'act', 'offer', 'accept', 'transfer', 'royalty', 'username', 'ethereumaddress', 'action', 'action-act', 'collection', 'collection-act'] 
        list_of_sn_fee_paying_ticket_types = ['act', 'action-act'] # See https://docs.pastel.network/basics/fees-and-burn
        for current_ticket_type in list_of_ticket_types:
            if verbose:
                log.info('Getting ' + current_ticket_type + ' tickets...')
            response = await rpc_connection.tickets('list', current_ticket_type)
            if response is not None and len(response) > 0:
                tickets_obj[current_ticket_type] = await get_df_json_from_tickets_list_rpc_response_func(response)
                if current_ticket_type in list_of_sn_fee_paying_ticket_types:
                    fee_paying_tickets_obj[current_ticket_type] = await get_df_json_from_tickets_list_rpc_response_func(response)
        return tickets_obj, fee_paying_tickets_obj

    async def get_df_json_from_tickets_list_rpc_response_func(rpc_response):
        tickets_df = pd.DataFrame.from_records([rpc_response[idx]['ticket'] for idx, x in enumerate(rpc_response)])
        tickets_df['txid'] = [rpc_response[idx]['txid'] for idx, x in enumerate(rpc_response)]
        tickets_df['height'] = [rpc_response[idx]['height'] for idx, x in enumerate(rpc_response)]
        tickets_df_json = tickets_df.to_json(orient='index')
        return tickets_df_json

    async def get_raw_transaction_func(txid, rpc_connection):
        raw_transaction_data = await rpc_connection.getrawtransaction(txid, 1) 
        return raw_transaction_data
    
    async def get_sending_addresses(current_raw_transaction_data, rpc_connection):
        sending_addresses = {}
        for vin in current_raw_transaction_data['vin']:
            prev_txid = vin['txid']
            prev_vout_index = vin['vout']
            # Fetch the previous transaction data
            prev_tx_data = await get_raw_transaction_func(prev_txid, rpc_connection)
            # Find the output in the previous transaction that corresponds to the current input
            if prev_vout_index < len(prev_tx_data['vout']):
                prev_vout = prev_tx_data['vout'][prev_vout_index]
                if 'addresses' in prev_vout.get('scriptPubKey', {}):
                    address = prev_vout['scriptPubKey']['addresses'][0]  # Assuming single address (change if needed)
                    value = prev_vout['value']
                    if address in sending_addresses:
                        sending_addresses[address] += value
                    else:
                        sending_addresses[address] = value
        return sending_addresses

    async def get_csv_of_burn_txids_from_explorer_func(use_testnet):
        log.info(f'Now retrieving all burn txids from Pastel explorer on {"testnet" if use_testnet else "mainnet"}...')
        burn_txids_df = None
        try:
            if use_testnet:
                csv_download_url = 'blob:https://explorer-testnet.pastel.network/e071bc1e-3749-437c-89a7-a4f759d9d062'
            else:
                csv_download_url = 'blob:https://explorer.pastel.network/93b9535b-65af-46a2-b35a-bfb91133f30d'
            burn_txids_df = pd.read_csv(csv_download_url)
            burn_txids_df['Datetime'] = pd.to_datetime(burn_txids_df['Timestamp'])
            return burn_txids_df
        except Exception as e:
            log.error(f"Error occurred in get_csv_of_burn_txids_from_explorer_func: {e}")
            return burn_txids_df

    async def determine_total_coins_burned_by_sending_address_func(burn_txids_df):
        log.info('Now determining total coins burned by sending address...')
        total_coins_burned_by_sending_address_dict = {}
        list_of_burn_transaction_txids = burn_txids_df['Hash'].tolist()
        pbar = tqdm(total=len(list_of_burn_transaction_txids))
        for current_txid in list_of_burn_transaction_txids:
            log.info('Processing burn txid: {}'.format(current_txid))
            pbar.set_description('Processing burn txid: {}'.format(current_txid))
            raw_tx_data = await get_raw_transaction_func(current_txid, rpc_connection)
            current_vout_data = raw_tx_data['vout']
            sending_address_for_current_txid = raw_tx_data['vin'][0]['prevout']['scriptPubKey']['addresses'][0]
            for current_vout in current_vout_data: #if receiving_address_for_current_txid == burn_address:
                if current_vout.get('scriptPubKey', {}).get('addresses') == [burn_address]:
                    if sending_address_for_current_txid not in total_coins_burned_by_sending_address_dict:
                        total_coins_burned_by_sending_address_dict[sending_address_for_current_txid] = current_vout['value']
                    else:
                        total_coins_burned_by_sending_address_dict[sending_address_for_current_txid] += current_vout['value']
            pbar.update(1)
        pbar.close()
        return total_coins_burned_by_sending_address_dict

    use_testnet = 0
    if use_testnet:
        burn_address = 'tPpasteLBurnAddressXXXXXXXXXXX3wy7u'
        url_string = f'https://explorer-testnet-api.pastel.network/v1/addresses/{burn_address}'
    else:
        burn_address = 'PtpasteLBurnAddressXXXXXXXXXXbJ5ndd'
        url_string = f'https://explorer-api.pastel.network/v1/addresses/{burn_address}'

    async with AsyncClient() as client:
        response = await client.get(url_string)
        address_data_dict = response.json()
    burn_address_balance = float(address_data_dict['incomingSum'])
    log.info(f'Burn address balance for {burn_address}: {burn_address_balance:,.2f}')
    
    use_get_coin_burn_by_sending_address = 1
    total_coins_burned_by_sending_address_dict = {}  # Initialize the dictionary

    if use_get_coin_burn_by_sending_address:
        try:
            burn_txids_df = await get_csv_of_burn_txids_from_explorer_func(use_testnet)
            total_coins_burned_by_sending_address_dict = await determine_total_coins_burned_by_sending_address_func(burn_txids_df)
        except Exception as e:
            log.error(f"Error occurred while getting total coins burned by sending address: {e}")

    try:        
        file_name_for_total_coins_burned_by_sending_address_dict = 'total_coins_burned_by_sending_address_dict.csv'
        pd.DataFrame.from_dict(total_coins_burned_by_sending_address_dict, orient='index').to_csv(file_name_for_total_coins_burned_by_sending_address_dict, header=['total_psl_burned'], index_label='sending_address')
    except Exception as e:
        log.error(f"Error occurred while writing total coins burned by sending address to csv file: {e}")

    tickets_obj, fee_paying_tickets_obj = await get_all_pastel_blockchain_tickets_func(verbose=1)
    list_of_table_names = list(tickets_obj.keys())
    list_of_fee_paying_table_names = list(fee_paying_tickets_obj.keys())

    all_txids = []
    for current_table_name in list_of_table_names:
        current_table = tickets_obj[current_table_name]
        if isinstance(current_table, str):
            current_table = pd.read_json(StringIO(current_table), orient='index')
        current_table_txids = current_table['txid'].tolist()
        all_txids.extend(current_table_txids)
    list_of_all_ticket_txids = list(set(all_txids))
    list_of_all_ticket_txids.sort()
    log.info(f'Number of unique ticket txids: {len(list_of_all_ticket_txids)}')
    
    all_fee_paying_txids = []
    for current_table_name in list_of_fee_paying_table_names:
        current_table = fee_paying_tickets_obj[current_table_name]
        if isinstance(current_table, str):
            current_table = pd.read_json(StringIO(current_table), orient='index')
        current_table_txids = current_table['txid'].tolist()
        all_fee_paying_txids.extend(current_table_txids)
    list_of_all_fee_paying_ticket_txids = list(set(all_fee_paying_txids))
    log.info(f'Number of unique fee paying ticket txids: {len(list_of_all_fee_paying_ticket_txids)}')    

    txid_to_total_burn_amount_dict = {}
    txid_to_total_fee_amount_dict = {}
    psl_address_to_total_coins_burned_dict = {}
    sn_collateral_address_to_fees_received_dict = {}
    sn_collateral_address_to_number_of_fee_paying_transactions_count_dict = {}
    pbar = tqdm(total=len(list_of_all_ticket_txids))
    for current_txid in list_of_all_ticket_txids:
        pbar.set_description(f'Processing txid: {current_txid}')
        current_raw_transaction_data = await get_raw_transaction_func(current_txid, rpc_connection)
        current_vout_data = current_raw_transaction_data['vout']
        total_burn_amount_for_current_txid = decimal.Decimal(0.0)
        sending_addresses = await get_sending_addresses(current_raw_transaction_data, rpc_connection)
        sending_address_for_current_txid = list(sending_addresses.keys())[0] if len(sending_addresses) > 0 else None
        for current_vout in current_vout_data:
            if current_vout.get('scriptPubKey', {}).get('type') == 'multisig':
                current_vout_amount = decimal.Decimal(current_vout['value'])
                total_burn_amount_for_current_txid += current_vout_amount
        txid_to_total_burn_amount_dict[current_txid] = total_burn_amount_for_current_txid
        if sending_address_for_current_txid:
            if sending_address_for_current_txid not in psl_address_to_total_coins_burned_dict:
                psl_address_to_total_coins_burned_dict[sending_address_for_current_txid] = total_burn_amount_for_current_txid
            else:
                psl_address_to_total_coins_burned_dict[sending_address_for_current_txid] += total_burn_amount_for_current_txid
        if current_txid in list_of_all_fee_paying_ticket_txids:
            total_fee_amount_for_current_txid = 0
            pubkeyhash_vout_counter = 0
            for current_vout in current_vout_data:
                if current_vout.get('scriptPubKey', {}).get('type') == 'pubkeyhash':
                    if pubkeyhash_vout_counter > 0:
                        current_vout_amount = decimal.Decimal(current_vout['value'])
                        total_fee_amount_for_current_txid += current_vout_amount
                        receiving_sn_collateral_address = current_vout['scriptPubKey']['addresses'][0]
                        if receiving_sn_collateral_address not in sn_collateral_address_to_fees_received_dict:
                            sn_collateral_address_to_fees_received_dict[receiving_sn_collateral_address] = current_vout_amount
                            sn_collateral_address_to_number_of_fee_paying_transactions_count_dict[receiving_sn_collateral_address] = 1
                        else:
                            sn_collateral_address_to_fees_received_dict[receiving_sn_collateral_address] += current_vout_amount
                            sn_collateral_address_to_number_of_fee_paying_transactions_count_dict[receiving_sn_collateral_address] += 1
                    pubkeyhash_vout_counter += 1
            txid_to_total_fee_amount_dict[current_txid] = total_fee_amount_for_current_txid
        pbar.update(1)
    pbar.close()
    log.info('Done processing all txids!')

    try:
        total_burned_in_dust_transactions = sum(txid_to_total_burn_amount_dict.values())
        log.info(f'Total burned in dust transactions: {total_burned_in_dust_transactions:,.2f}')
        total_burned_psl = float(total_burned_in_dust_transactions) + burn_address_balance
        log.info(f'Total burned psl overall (dust transactions and sent to burn address): {total_burned_psl:,.2f}')
        total_fees_paid_to_sns = sum(txid_to_total_fee_amount_dict.values())
        log.info('Total fees paid to SNs: {}'.format(total_fees_paid_to_sns))        
    except Exception as e:
        log.error(f"Error occurred while calculating total burned psl: {e}")

    try:
        log.info('Writing total burned psl to text file...')
        with open('total_burned_psl.txt', 'w') as f:
            f.write(str(total_burned_psl))
        log.info('Done!')
    except Exception as e:
        log.error(f"Error occurred while writing total burned psl to text file: {e}")
    
    try:
        log.info('Writing total psl paid in fees to SNs to text file...')
        with open('total_psl_paid_to_sns.txt', 'w') as f:
            f.write(str(total_burned_psl))
        log.info('Done!')
    except Exception as e:
        log.error(f"Error occurred while writing total psl paid in fees to SNs to text file: {e}")

    try:
        log.info('Writing total fees paid to each SN to csv file...')
        output_file_name_for_total_fees_paid_to_each_sn_csv_file = 'total_fees_paid_to_each_sn.csv'
        pd.DataFrame.from_dict(sn_collateral_address_to_fees_received_dict, orient='index').to_csv(output_file_name_for_total_fees_paid_to_each_sn_csv_file, header=['total_psl_received_as_fees'], index_label='sn_collateral_address')
        
        log.info('Writing total fee-paying transaction to each SN to csv file...')
        output_file_name_for_total_fee_paying_transactions_to_each_sn_csv_file = 'total_fee_paying_transactions_to_each_sn.csv'
        pd.DataFrame.from_dict(sn_collateral_address_to_number_of_fee_paying_transactions_count_dict, orient='index').to_csv(output_file_name_for_total_fee_paying_transactions_to_each_sn_csv_file, header=['number_of_fee_paying_transactions'], index_label='sn_collateral_address')
    except Exception as e:
        log.error(f"Error occurred while writing total fees paid to each SN to csv file: {e}")
    
# Run the main coroutine
if __name__ == "__main__":
    asyncio.run(main())
