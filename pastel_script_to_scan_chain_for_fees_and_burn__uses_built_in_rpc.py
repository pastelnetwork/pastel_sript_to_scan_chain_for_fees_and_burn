import logging
import asyncio
import json
import base64
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
import urllib.parse as urlparse
from httpx import AsyncClient, Limits, Timeout

# Setup logging:
log = logging.getLogger("PastelExplorerBurnAndFeeTracker")
log.propagate = False
for handler in log.handlers[:]:
    log.removeHandler(handler)
log.setLevel(logging.INFO)
log_folder = Path('application_log_files')
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
        elif line == '\n':
            pass
        else:
            current_flag = line.strip().split('=')[0].strip()
            current_value = line.strip().split('=')[1].strip()
            other_flags[current_flag] = current_value
    return rpchost, rpcport, rpcuser, rpcpassword, other_flags

class AsyncAuthServiceProxy:
    def __init__(self, service_url, request_timeout=20):
        self.service_url = service_url
        self.url = urlparse.urlparse(service_url)
        self.client = AsyncClient(timeout=Timeout(request_timeout), limits=Limits(max_connections=200, max_keepalive_connections=10))
        user = self.url.username
        password = self.url.password
        authpair = f"{user}:{password}".encode('utf-8')
        self.auth_header = b'Basic ' + base64.b64encode(authpair)

    async def generate_report(self, report_name):
        postdata = json.dumps({
            'jsonrpc': '1.0',
            'id': 'generate-report',
            'method': 'generate-report',
            'params': [report_name]
        })
        headers = {
            'Authorization': self.auth_header,
            'Content-type': 'application/json'
        }
        response = await self.client.post(self.service_url, headers=headers, data=postdata)
        return response.json()

async def main():
    rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
    rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")

    report = await rpc_connection.generate_report('fees-and-burn')

    result = report.get('result', {})
    summary = result.get('summary', {})
    sn_statistics = result.get('snStatistics', [])
    address_coin_burn = result.get('addressCoinBurn', {})

    log.info(f'Summary: {summary}')
    log.info(f'SN Statistics: {sn_statistics}')
    log.info(f'Address Coin Burn: {address_coin_burn}')

    total_burned_in_dust_transactions = summary.get('totalBurnedInDustTransactions', 0)
    total_fees_paid_to_sns = summary.get('totalFeesPaidToSNs', 0)
    burn_address_balance = sum(address_coin_burn.values())
    total_burned_psl = total_burned_in_dust_transactions + burn_address_balance

    log.info(f'Total burned in dust transactions: {total_burned_in_dust_transactions:,.2f}')
    log.info(f'Total burned psl overall (dust transactions and sent to burn address): {total_burned_psl:,.2f}')
    log.info(f'Total fees paid to SNs: {total_fees_paid_to_sns:,.2f}')

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
            f.write(str(total_fees_paid_to_sns))
        log.info('Done!')
    except Exception as e:
        log.error(f"Error occurred while writing total psl paid in fees to SNs to text file: {e}")

    try:
        log.info('Writing total fees paid to each SN to csv file...')
        sn_statistics_df = pd.DataFrame(sn_statistics)
        log.info(f'SN Statistics DataFrame:\n{sn_statistics_df}')
        if not sn_statistics_df.empty:
            sn_statistics_df.to_csv('total_fees_paid_to_each_sn.csv', index=False)
        else:
            log.warning("SN statistics dataframe is empty. Skipping writing to total_fees_paid_to_each_sn.csv")

        log.info('Writing total fee-paying transactions to each SN to csv file...')
        if 'address' in sn_statistics_df.columns and 'feePayingTransactionCount' in sn_statistics_df.columns:
            fee_paying_transactions_df = sn_statistics_df[['address', 'feePayingTransactionCount']].copy()
            fee_paying_transactions_df.columns = ['sn_collateral_address', 'number_of_fee_paying_transactions']
            fee_paying_transactions_df.to_csv('total_fee_paying_transactions_to_each_sn.csv', index=False)
        else:
            log.warning("Required columns are missing in SN statistics dataframe. Skipping writing to total_fee_paying_transactions_to_each_sn.csv")
    except Exception as e:
        log.error(f"Error occurred while writing total fees paid to each SN to csv file: {e}")

# Run the main coroutine
if __name__ == "__main__":
    asyncio.run(main())
