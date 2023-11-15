# Pastel Network Blockchain Fee and Burn Analysis Script

## Introduction
This script is a specialized tool designed for the Pastel Network. Its primary function is to analyze transactions on the Pastel blockchain to calculate and report on fees and coin burns. It achieves this by scanning blockchain tickets and aggregating data related to transaction fees and the burning of coins (a process of permanently removing coins from circulation).

## Functional Overview
The script operates through several key steps:
1. **Connection to Pastel Network**: It connects to a Pastel blockchain node via RPC, using settings defined in `pastel.conf`.
2. **Retrieval of Blockchain Tickets**: Various ticket types, such as `id`, `nft`, `act`, etc., are fetched and processed.
3. **Analysis of Transactions for Burn and Fees**:
   - Identifies transactions where coins were sent to a burn address.
   - Calculates fees associated with specific ticket transactions.
   - Aggregates this data to provide a comprehensive overview of coin burns and fees within the network.

## Key Components
- **AsyncAuthServiceProxy**: Manages asynchronous RPC calls to the Pastel node.
- **get_all_pastel_blockchain_tickets_func**: Retrieves all ticket types from the blockchain and processes them into DataFrame objects.
- **get_sending_addresses**: Extracts sending addresses from transaction data.
- **determine_total_coins_burned_by_sending_address_func**: Calculates the total amount of coins burned by each sending address.
- **Data Aggregation and Export**: The script aggregates the collected data and exports it into CSV and text files for easy analysis and record-keeping.

## Requirements
- Python 3.6+
- Dependencies: `pandas`, `httpx`, `tqdm`

## Setup and Execution
1. Ensure Python 3.6+ is installed.
2. Configure RPC settings in `~/.pastel/pastel.conf` with appropriate `rpcuser`, `rpcpassword`, `rpcport`, and `rpchost`.
3. Clone and install project in a virtual environment as follows:

```bash
git clone https://github.com/pastelnetwork/pastel_script_to_scan_chain_for_fees_and_burn
cd pastel_script_to_scan_chain_for_fees_and_burn
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install wheel
pip install -r requirements.txt
```

4. Run the script using the command `python3 pastel_script_to_scan_chain_for_fees_and_burn.py` from the project directory.

## Output Files
- **CSV Files**: Contain detailed information about coins burned and fees paid.
- **Text Files**: Summarize the total amount of PSL burned and fees paid to SNs.
- **Logs**: Document the script's operational details and any errors encountered.

## Notes
- The script uses asynchronous calls for efficient data processing.
- It generates detailed logs to aid in troubleshooting and understanding script operations.

# Pastel Network Blockchain Analysis Script - README

## Introduction
This script is a comprehensive tool designed for the Pastel Network. It connects to the Pastel blockchain, retrieves various types of blockchain tickets, processes transactions, and performs detailed analyses to calculate and report on the burning of coins (coin burn) and the fees paid to Supernodes.

## Script Workflow and Rationale

### Initial Setup and RPC Connection
- **RPC Connection**: The script begins by establishing a Remote Procedure Call (RPC) connection to the Pastel Network. It extracts the local RPC settings, including host, port, user, and password, from the `pastel.conf` file. The `AsyncAuthServiceProxy` object is used for making asynchronous RPC calls, which is crucial for handling multiple simultaneous network requests efficiently.

### Retrieving and Processing Blockchain Tickets
- **Function: `get_all_pastel_blockchain_tickets_func`**:
  - **Purpose**: To fetch various types of blockchain tickets from the network. Blockchain tickets in Pastel are unique data structures that serve different functions, like identifying users (PastelID tickets) or representing assets (NFT tickets).
  - **Process**: Iterates through a predefined list of ticket types, including standard and fee-paying tickets (e.g., `act`, `action-act`), and retrieves them using RPC calls. Each ticket type's response is then processed to convert it into a JSON format for easier analysis.
  - **Rationale**: This function is key for aggregating detailed information about transactions and activities on the Pastel Network, which forms the foundation for the subsequent analysis of coin burns and fees.

### Analyzing Transactions for Coin Burns and Fees
- **Function: `get_raw_transaction_func` and `get_sending_addresses`**:
  - **Purpose**: To fetch detailed information about each transaction and identify the sending addresses.
  - **Process**: For each transaction ID obtained from the tickets, the script fetches the raw transaction data, which includes inputs (`vin`) and outputs (`vout`). It then traces back the sending addresses and the amounts involved in these transactions.
  - **Rationale**: Understanding the flow of coins in each transaction is critical for identifying which part of the transaction constitutes a coin burn or a fee payment.

### Coin Burn Analysis
- **Function: `determine_total_coins_burned_by_sending_address_func`**:
  - **Purpose**: To calculate the total amount of Pastel coins (PSL) burned by each address.
  - **Process**: It uses a list of transaction IDs specifically associated with coin burns, obtained from the Pastel blockchain explorer. For each transaction, it determines the amount of PSL burned and aggregates this by the sending address.
  - **Rationale**: Coin burn is a mechanism to reduce the supply of PSL, usually as a part of a deflationary model or for other economic reasons. This function provides insights into how much PSL is being removed from circulation and by whom.

### Data Aggregation and Output
- **Final Steps**:
  - **Process**: After processing all the transactions, the script aggregates the total PSL burned and the fees paid. This data is then output to various files (CSV and text formats) for easy access and analysis. The script also logs the total number of unique ticket transaction IDs processed, providing a sense of the scale of the data analyzed.
  - **Rationale**: Aggregating and exporting this data is essential for allowing network participants and analysts to review and use this information for economic modeling, network health assessment, or other analytical purposes.

### Logging Setup
- **Logging Configuration**: The script configures a logger (`log`) specifically for this application (`"PastelExplorerBurnAndFeeTracker"`). This logger will capture and record events, errors, and other significant occurrences during the script's execution.
- **Rotating File Handler**: A `RotatingFileHandler` is set up for the logger. This means the log files will automatically rotate (i.e., a new log file is started) when the current file reaches a certain size (5 MB in this case). It keeps the last 10 log files as backups.
- **Logging Format**: A specific format is defined for log messages, which includes the timestamp, logger name, log level, and the message. This format is applied to both the file handler and the standard output (console).

### RPC Settings Extraction
- **Function: `get_local_rpc_settings_func`**: 
  - **Purpose**: To extract the local RPC configuration from the `pastel.conf` file, typically located in the user's home directory. This configuration is essential for connecting to the Pastel blockchain.
  - **Process**: It reads the configuration file and parses out the necessary details like `rpcport`, `rpcuser`, `rpcpassword`, and other flags.

### JSON RPC Exception Handling
- **Custom Exception: `JSONRPCException`**: 
  - **Purpose**: To define a custom exception that handles errors specifically related to JSON RPC calls. This is important for gracefully handling and logging RPC-related errors.

### Asynchronous RPC Proxy
- **Class: `AsyncAuthServiceProxy`**:
  - **Purpose**: To handle asynchronous communication with the Pastel Network's RPC server. It manages the complexities of making RPC calls, including authorization, request formatting, and response handling.
  - **Concurrency Control**: Uses a semaphore to limit the maximum number of concurrent RPC requests, ensuring that the system does not get overwhelmed.
  - **Reconnection Strategy**: Implements a reconnection mechanism, which is crucial for maintaining a stable connection to the RPC server. If a request fails, it will retry with an increasing delay between attempts.
  - **Request Handling**: The `__call__` method constructs and sends the RPC requests. It serializes the parameters into JSON, sets the necessary headers (including authorization), and manages the sending and receiving of requests and responses.

## Conclusion
This script is an essential tool for anyone looking to perform in-depth analysis of the Pastel Network's blockchain transactions. It provides valuable insights into the economic activities within the network, particularly focusing on coin burn mechanisms and Supernode fee structures.