import requests
import csv
import os
import time
import random
import datetime as dt
import signal
import sys
from dotenv import load_dotenv
import json
from web3 import Web3
import logging
from logging.handlers import RotatingFileHandler
import concurrent.futures
import threading

############################
# GLOBAL CONFIG
############################

# Toggle to False to exit loop after first quote
TOGGLE = True

# File and directory configuration
SAVE_DIR = "/Users/{}/Documents/uniswap_quotes/"
FILE_VERSION = "v1" # Updated file version
LOG_DIR = "/Users/{}/Documents/uniswap_quotes/logs"

# Quotes configuration (excluding pairs)
USD_NOTIONALS = [500, 2000, 10000]
POOL_FEE_TIERS = [100, 500, 3000, 10000]  # 0.01%, 0.05%, 0.3%, 1%
INTERFACE_FEE_PCT = 0.0025  # Uniswap interface fee (0.25%)


# Network-specific configuration
ETHEREUM_URL_TEMPLATE = "https://mainnet.infura.io/v3/{}"

# Parallelization
MAX_WORKERS_ETHEREUM = 4 
ENABLE_PARALLEL_PROCESSING = True

# ETH price cache configuration
ETH_PRICE_CACHE_DURATION = 300  # 5 minutes in seconds

# Higher slippage tolerance for lower liquidity tokens
SLIPPAGE_TOLERANCE_BY_TOKEN = {
   'WBTC': 0.05,     # 5%
   'ETH': 0.05,      # 5%
   'default': 0.10   # 10% for everything else
}

# Generate timestamped filenames
current_date = dt.datetime.now().strftime("%Y%m%d")
csv_filename = f"uniswap_quotes_{FILE_VERSION}_{current_date}.csv"
CSV_FILE = os.path.join(SAVE_DIR, csv_filename)
log_file = os.path.join(LOG_DIR, f"uniswap_quotes_{FILE_VERSION}_{current_date}.log")

# Ensure directories exist
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

##################
# LOGGING
##################

# Create logger with rotating file handler
logger = logging.getLogger("uniswap_quotes")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

############################
# WEB3 CONFIG
############################

# Load environment variables
load_dotenv(dotenv_path="../.env")
infura_api_key = os.getenv("INFURA_API_KEY")
logger.info(f"Infura API key loaded: {bool(infura_api_key)}")

# Initialize Web3 connections
ETHEREUM_URL = ETHEREUM_URL_TEMPLATE.format(infura_api_key)
w3_ethereum = Web3(Web3.HTTPProvider(ETHEREUM_URL))

############################
# TOKEN CONFIG
############################

# Token decimals (consistent across networks)
TOKEN_DECIMALS = {
   "ETH": 18, "USDC": 6, "USDT": 6, "WBTC": 8, "LINK": 18, "AAVE": 18 # 
}

############################
# NETWORK CONFIG
############################

NETWORK_CONFIGS = {
   "ethereum": {
       "w3": w3_ethereum,
       "name": "Ethereum Mainnet",
       "tokens": { # Reduced to relevant tokens
           "ETH": w3_ethereum.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),   # WETH
           "USDC": w3_ethereum.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),  # USDC
           "USDT": w3_ethereum.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7"),  # USDT 
           "WBTC": w3_ethereum.to_checksum_address("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"),  # WBTC
           "LINK": w3_ethereum.to_checksum_address("0x514910771AF9Ca656af840dff83E8264EcF986CA"),  # LINK 
           "AAVE": w3_ethereum.to_checksum_address("0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9")   # AAVE
       },
       "trade_pairs": [ 
           ("USDC", "ETH"), ("USDT", "ETH"),
           ("USDC", "WBTC"), ("USDT", "WBTC"),
           ("USDC", "LINK"), ("USDT", "LINK"), # 
           ("USDC", "AAVE"), ("USDT", "AAVE")
       ],
       "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
       "quoter_address": "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
       "quoter_v2_address": "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
   }
}


# ETH price cache to avoid excessive API calls
eth_price_cache = {"price": None, "timestamp": None, "cache_duration": ETH_PRICE_CACHE_DURATION}

# Thread-safe CSV writing
csv_lock = threading.Lock()

################################
# NETWORK CONNECTION
################################

def  validate_network_connections ():
   """Validate connections to all configured networks"""
   for network_key, network_info in NETWORK_CONFIGS.items():
       try:
           connected = network_info["w3"].is_connected()
           logger.info(f"{network_info['name']} connection status: {connected}")
           if not connected:
               logger.error(f"Could not connect to {network_info['name']}. Please check your connection.") # 
               sys.exit(1)
       except Exception as e:
           logger.error(f"Error connecting to {network_info['name']}: {e}")
           sys.exit(1)

# Validate connections
validate_network_connections()

###############################
# SHUTDOWN
###############################

def  signal_handler (sig, frame):
   """Handle graceful shutdown on interrupt"""
   logger.info("Shutting down quote collector...")
   sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

############################
# SET UP ABIs
############################

# Uniswap V3 Factory contract - returns the pool address for a given pair and fee 
FACTORY_ABI = json.loads("""
[
 {
   "inputs": [
     {"internalType": "address", "name": "tokenA", "type": "address"},
     {"internalType": "address", "name": "tokenB", "type": "address"},
     {"internalType": "uint24", "name": "fee", "type": "uint24"}
   ],
   "name": "getPool",
   "outputs": [{"internalType": "address", "name": "pool", "type": "address"}],
   "stateMutability": "view",
   "type": "function"
 }
]
""")

# Uniswap Pool ABI for confirming fee tier and liquidity
POOL_ABI = json.loads("""
[
 {
   "inputs": [],
   "name": "fee",
   "outputs": [{"internalType": "uint24", "name": "", "type": "uint24"}],
   "stateMutability": "view",  
   "type": "function"
 },
 {
   "inputs": [],
   "name": "liquidity",
   "outputs": [{"internalType": "uint128", "name": "", "type": "uint128"}],
   "stateMutability": "view",
   "type": "function"
 }
]
""")

# Uniswap v3 Quoter contract to simulate the swap
QUOTER_ABI = json.loads("""
[
 {
   "inputs": [
     {"internalType": "address", "name": "tokenIn", "type": "address"},
     {"internalType": "address", "name": "tokenOut", "type": "address"},
     {"internalType": "uint24", "name": "fee", "type": "uint24"},
     {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
     {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"}
   ],  
   "name": "quoteExactInputSingle",
   "outputs": [
     {"internalType": "uint256", "name": "amountOut", "type": "uint256"}
   ],
   "stateMutability": "nonpayable",
   "type": "function"
 },
 {
   "inputs": [
     {"internalType": "bytes", "name": "path", "type": "bytes"},
     {"internalType": "uint256", "name": "amountIn", "type": "uint256"}
   ],
   "name": "quoteExactInput",
   "outputs": [
     {"internalType": "uint256", "name": "amountOut", "type": "uint256"}
   ],
   "stateMutability": "nonpayable",
   "type": "function"
 }
]
""")

# Uniswap V3 QuoterV2 contract to get the gas estimates
QUOTER_V2_ABI = json.loads("""
[
 {  
   "inputs": [
     {"internalType": "bytes", "name": "path", "type": "bytes"},
     {"internalType": "uint256", "name": "amountIn", "type": "uint256"}
   ],
   "name": "quoteExactInput",
   "outputs": [
     {"internalType": "uint256", "name": "amountOut", "type": "uint256"},
     {"internalType": "uint160[]", "name": "sqrtPriceX96AfterList", "type": "uint160[]"},
     {"internalType": "uint32[]", "name": "initializedTicksCrossedList", "type": "uint32[]"},
     {"internalType": "uint256", "name": "gasEstimate", "type": "uint256"}
   ],
   "stateMutability": "nonpayable",
   "type": "function"
 }
]
""")

############################
# CONTRACT INITIALIZATION
############################

def  initialize_network_contracts ():
   """Initialize smart contracts for all configured networks"""
   for network_key, config in NETWORK_CONFIGS.items():
       w3_instance = config["w3"]
      
       # Factory contract
       factory_address = w3_instance.to_checksum_address(config["factory_address"])
       config["factory"] = w3_instance.eth.contract(address=factory_address, abi=FACTORY_ABI)
      
       # Quoter contract (only if address exists)
       if config["quoter_address"]:
           quoter_address = w3_instance.to_checksum_address(config["quoter_address"])
           config["quoter"] = w3_instance.eth.contract(address=quoter_address, abi=QUOTER_ABI) # 
           config["quoter_available"] = True
       else:
           config["quoter"] = None
           config["quoter_available"] = False
           logger.info(f"QuoterV1 not available for {config['name']} (using QuoterV2 only)")
      
       # QuoterV2 contract (with error handling)
       try:
           quoter_v2_address = w3_instance.to_checksum_address(config["quoter_v2_address"]) # 
           config["quoter_v2"] = w3_instance.eth.contract(address=quoter_v2_address, abi=QUOTER_V2_ABI)
           config["quoter_v2_available"] = True
           logger.info(f"QuoterV2 contract initialized successfully for {config['name']}")
       except Exception as e:
           config["quoter_v2_available"] = False
           logger.warning(f"Warning: QuoterV2 contract not available for {config['name']}: {e}")

# Initialize contracts on startup
initialize_network_contracts()

############################
# FUNCTIONS 
############################

def  get_current_eth_price ():
   """
   Get current ETH price in USD from CoinGecko API and cache it
   """
   current_time = time.time()
  
   # Check if we have a cached price that's still valid
   if (eth_price_cache["price"] is not None and
       eth_price_cache["timestamp"] is not None and
       current_time - eth_price_cache["timestamp"] < eth_price_cache["cache_duration"]):
       return eth_price_cache["price"]
  
   try:
       # Fetch from CoinGecko API
       response = requests.get( # 
           "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
           timeout=10
       )
       response.raise_for_status()
       data = response.json()
      
       eth_price = data["ethereum"]["usd"]
      
       # Update cache
       eth_price_cache["price"] = eth_price
       eth_price_cache["timestamp"] = current_time
       logger.info(f"Fetched current ETH price: ${eth_price}") # 
       return eth_price
      
   except Exception as e:
       logger.warning(f"Failed to fetch ETH price from CoinGecko: {e}")
      
       # Fallback to cached price if available
       if eth_price_cache["price"] is not None:
           logger.info(f"Using cached ETH price: ${eth_price_cache['price']}")
           return eth_price_cache["price"] # 
      
       # Final fallback to approximate price
       logger.warning("Using fallback ETH price of $3000")
       return 3000

def  write_quote_row (row):
   """Write a quote to the CSV file in a thread-safe manner"""
   with csv_lock:
       file_exists = os.path.isfile(CSV_FILE)
       with open(CSV_FILE, "a", newline="") as f:
           writer = csv.DictWriter(f, fieldnames=row.keys()) # 
           if not file_exists:
               writer.writeheader()
           writer.writerow(row)

def  detect_unreasonable_slippage (amount_in_small, amount_out_small, amount_in_large, amount_out_large, token_symbol=None):
   """
   Simulate slippage between small and large trades
   """
   if amount_in_small == 0 or amount_out_small == 0:
       return False # 
  
   # Get token-specific slippage tolerance
   max_slippage_ratio = SLIPPAGE_TOLERANCE_BY_TOKEN.get(token_symbol, SLIPPAGE_TOLERANCE_BY_TOKEN['default'])
  
   # Calculate price per unit for both trades
   price_small = amount_out_small / amount_in_small
   price_large = amount_out_large / amount_in_large
  
   # Calculate the ratio of input amounts
   input_ratio = amount_in_large / amount_in_small
  
   # Calculate the ratio of output amounts
   output_ratio = amount_out_large / amount_out_small
  
   # If output ratio is significantly less than input ratio, we have excessive slippage
   slippage_ratio = 1 - (output_ratio / input_ratio) # 
  
   logger.info(f"Slippage analysis for {token_symbol or 'unknown'}: input_ratio={input_ratio:.2f}, output_ratio={output_ratio:.2f}, slippage_ratio={slippage_ratio:.4f}, tolerance={max_slippage_ratio:.2f}")
  
   return slippage_ratio > max_slippage_ratio

# Uniswap Functions
def  find_best_pool_with_liquidity (token_in, token_out, amount_in, token_in_symbol, network="ethereum"):
   """
   Find the pool with the lowest fee tier that has sufficient liquidity based on slippage check
   """
   config = NETWORK_CONFIGS[network]
   w3_instance = config["w3"]
   factory_contract = config["factory"]
  
   decimals_in = TOKEN_DECIMALS[token_in_symbol]
  
   logger.info(f"[{config['name']}] Looking for available pools for {token_in_symbol}")
  
   # Check all fee tiers from lowest to highest
   for fee in POOL_FEE_TIERS:
       try:
           pool_address = factory_contract.functions.getPool(token_in, token_out, fee).call()
           if pool_address != "0x0000000000000000000000000000000000000000":
               logger.info(f"[{config['name']}] Found pool with fee tier {fee/10000}% at {pool_address}") # 
              
               try: # 
                   pool_contract = w3_instance.eth.contract(address=pool_address, abi=POOL_ABI)
                   total_liquidity = pool_contract.functions.liquidity().call()
                  
                   # Basic sanity check - just ensure liquidity > 0
                   if total_liquidity > 0: # 
                       logger.info(f"[{config['name']}] Pool has liquidity ({total_liquidity}). Using fee tier {fee/10000}%") # 
                       return fee, pool_address
                   else:
                       logger.info(f"[{config['name']}] Pool has zero liquidity, trying next fee tier")
                       continue # 
                      
               except Exception as liquidity_error:
                   logger.warning(f"[{config['name']}] Could not check liquidity for pool {pool_address}: {liquidity_error}")
                   # If we can't check liquidity, still try this pool
                   logger.info(f"[{config['name']}] Using pool despite liquidity check error. Fee tier {fee/10000}%") # 
                   return fee, pool_address
                  
       except Exception as e:
           logger.error(f"[{config['name']}] Error checking pool for fee tier {fee}: {e}")
  
   # No pools found across all fee tiers 
   logger.warning(f"[{config['name']}] No pools found for {token_in_symbol} across all fee tiers - skipping pair")
   return None, None

def  validate_quote_with_slippage_check (token_in, token_out, current_fee_tier, amount_in, amount_out,
                                    token_in_symbol, quoter_contract, quoter_available,
                                    quoter_v2_contract, quoter_v2_available, config): 
   try:
       # Test with 10% of the amount to check for slippage 
       small_amount_in = int(amount_in * 0.1)
       small_amount_out = None
      
       if not quoter_available and quoter_v2_available:
           # Use QuoterV2 for slippage check 
           fee_hex = current_fee_tier.to_bytes(3, byteorder='big').hex()
           path = token_in.replace('0x', '') + fee_hex + token_out.replace('0x', '')
           path = '0x' + path.lower()
           path_bytes = Web3.to_bytes(hexstr=path)
           small_amount_out, _, _, _ = quoter_v2_contract.functions.quoteExactInput( # 
               path_bytes, small_amount_in
           ).call()
       elif quoter_available:
           # Use QuoterV1 for slippage check
           small_amount_out = quoter_contract.functions.quoteExactInputSingle(
               token_in, token_out, current_fee_tier, small_amount_in, 0
           ).call()
      
       if small_amount_out:
           # Calculate slippage 
           input_ratio = amount_in / small_amount_in
           output_ratio = amount_out / small_amount_out
           slippage_ratio = 1 - (output_ratio / input_ratio)
           slippage_percentage = slippage_ratio * 100  # Convert to percentage
          
           # Check if slippage is unreasonable 
           max_slippage_ratio = SLIPPAGE_TOLERANCE_BY_TOKEN.get(token_in_symbol, SLIPPAGE_TOLERANCE_BY_TOKEN['default'])
           is_reasonable = slippage_ratio <= max_slippage_ratio
          
           if not is_reasonable:
               logger.warning(f"[{config['name']}] Detected unreasonable slippage for {token_in_symbol} at fee tier {current_fee_tier/10000}%: {slippage_percentage:.2f}%")
           return is_reasonable, slippage_percentage # 
       else:
           # If we can't get small quote, assume no slippage issue
           return True, 0.0
      
   except Exception as slippage_check_error:
       logger.warning(f"[{config['name']}] Could not perform slippage check: {slippage_check_error}")
       # If we can't check slippage, assume the quote is valid and no slippage data
       return True, None

def  get_uniswap_quote (token_in_symbol, token_out_symbol, notional_amount, gas_price=None, network="ethereum"): # 
   """
   Get quote from Uniswap for a given token pair and amount, selecting the best pool
   """
   config = NETWORK_CONFIGS[network]
   w3_instance = config["w3"]
   quoter_contract = config.get("quoter")
   quoter_available = config.get("quoter_available", False)
   quoter_v2_contract = config.get("quoter_v2")
   quoter_v2_available = config.get("quoter_v2_available", False)
   tokens = config["tokens"]
  
   # Check if we have at least one quoter available
   if not quoter_available and not quoter_v2_available:
       error_msg = f"[{config['name']}] No quoter contracts available" # 
       logger.error(error_msg)
       raise Exception(error_msg)
  
   token_in = tokens[token_in_symbol]
   token_out = tokens[token_out_symbol]
   decimals_in = TOKEN_DECIMALS[token_in_symbol]
   decimals_out = TOKEN_DECIMALS[token_out_symbol]
   amount_in = int(notional_amount * 10**decimals_in)
  
   # Find the best pool with sufficient liquidity
   fee_tier, pool_address = find_best_pool_with_liquidity(token_in, token_out, amount_in, token_in_symbol, network)
  
   # Check if any pools were found
   if fee_tier is None:
       error_msg = f"[{config['name']}] No pools available for {token_in_symbol}-{token_out_symbol} - skipping pair" # 
       logger.warning(error_msg)
       raise Exception(error_msg)
  
   # Try the selected fee tier first, then fallback to others if needed
   fee_tiers_to_try = [fee_tier] + [f for f in POOL_FEE_TIERS if f != fee_tier]
  
   last_error = None
   for current_fee_tier in fee_tiers_to_try:
       try:
           amount_out = None
           gas_estimate = 150000  # Default fallback 
          
           # Use QuoterV2 if QuoterV1 is not available
           if quoter_v2_available and not quoter_available:
               # Create the path for QuoterV2
               fee_hex = current_fee_tier.to_bytes(3, byteorder='big').hex()
               path = token_in.replace('0x', '') + fee_hex + token_out.replace('0x', '') # 
               path = '0x' + path.lower()
               path_bytes = Web3.to_bytes(hexstr=path)
              
               # QuoterV2 returns (amountOut, sqrtPriceX96AfterList, initializedTicksCrossedList, gasEstimate)
               amount_out, _, _, gas_estimate = quoter_v2_contract.functions.quoteExactInput( # 
                   path_bytes, amount_in
               ).call()
               logger.info(f"[{config['name']}] Uniswap: Using QuoterV2 for fee tier {current_fee_tier/10000}% for {token_in_symbol}-{token_out_symbol}")
          
           # Use QuoterV1 if available
           elif quoter_available:
               amount_out = quoter_contract.functions.quoteExactInputSingle( # 
                   token_in, token_out, current_fee_tier, amount_in, 0
               ).call()
               logger.info(f"[{config['name']}] Uniswap: Using QuoterV1 for fee tier {current_fee_tier/10000}% for {token_in_symbol}-{token_out_symbol}")
              
               # Try to get gas estimate from QuoterV2 if available and QuoterV1 was used for amount_out 
               if quoter_v2_available and quoter_v2_contract:
                   try:
                       fee_hex = current_fee_tier.to_bytes(3, byteorder='big').hex()
                       path = token_in.replace('0x', '') + fee_hex + token_out.replace('0x', '') # 
                       path = '0x' + path.lower()
                       path_bytes = Web3.to_bytes(hexstr=path)
                       _, _, _, gas_estimate_v2 = quoter_v2_contract.functions.quoteExactInput( # 
                           path_bytes, amount_in
                       ).call()
                       gas_estimate = gas_estimate_v2 # Update gas estimate if successful
                       logger.info(f"[{config['name']}] Uniswap: Gas estimate from QuoterV2: {gas_estimate}")
                   except Exception:
                       pass # Silently fall back to default estimate if QuoterV2 gas fails 
          
           if amount_out is None:
               logger.error(f"[{config['name']}] Uniswap: Could not get amountOut using available quoters for {token_in_symbol}-{token_out_symbol} at fee {current_fee_tier/10000}%")
               raise Exception("No quoter available or quoter failed to return amount out")
          
           # Validate quote with slippage check for all trades 
           is_valid_quote, slippage_percentage = validate_quote_with_slippage_check(
               token_in, token_out, current_fee_tier, amount_in, amount_out,
               token_in_symbol, quoter_contract, quoter_available,
               quoter_v2_contract, quoter_v2_available, config
           )
           
           if not is_valid_quote: # 
               logger.warning(f"[{config['name']}] Quote failed slippage validation for {token_in_symbol}-{token_out_symbol} at fee tier {current_fee_tier/10000}%, trying next tier")
               last_error = Exception(f"Slippage validation failed: {slippage_percentage:.2f}%") # Store this as a potential error
               continue
          
           receiving = amount_out / (10 ** decimals_out)
           price = notional_amount / receiving if receiving > 0 else 0 # 
           pool_fee_pct = current_fee_tier / 1000000 # Uniswap pool fee is fee_tier / 1,000,000 (e.g. 3000 / 1M = 0.3%)
           pool_fee_amount = notional_amount * pool_fee_pct
           interface_fee_amount = notional_amount * INTERFACE_FEE_PCT
          
           if gas_price is None: # 
               gas_price = w3_instance.eth.gas_price
              
           gas_cost_eth = (gas_estimate * gas_price) / 10**18
           eth_price_usd = get_current_eth_price() # 
           gas_cost_usd = gas_cost_eth * eth_price_usd

           chain_id = 1 # Ethereum Mainnet chain ID

           return {
               "timestamp": dt.datetime.now(dt.UTC).isoformat(),
               "quoter": "uniswap_quoter", # 
               "aggregator": "uniswap",
               "network": network,
               "token_in_chain_id": chain_id,
               "token_out_chain_id": chain_id,
               "direction": f"{token_in_symbol}->{token_out_symbol}",
               "notional": notional_amount, # 
               "selling": notional_amount,
               "token_in_symbol": token_in_symbol,
               "token_in_address": token_in,
               "receiving": receiving,
               "token_out_symbol": token_out_symbol,
               "token_out_address": token_out, # 
               "amount_in": str(amount_in),
               "amount_out": str(amount_out),
               "amount_out_decimals": str(receiving),
               "price": price,
               "interface_fee": interface_fee_amount, # 
               "gas_compute": str(gas_estimate),
               "gas_cost_eth": str(gas_cost_eth),
               "gas_cost_usd": str(gas_cost_usd),
               "effective_price": str((notional_amount + interface_fee_amount + gas_cost_usd)/receiving if receiving > 0 else 0),
               "pool_address": pool_address if pool_address else "unknown",
               "fee_tier": current_fee_tier, # 
               "pool_fee": pool_fee_amount,
               "slippage_percentage": str(slippage_percentage) if slippage_percentage is not None else "Missing"
           }
       except Exception as e: # 
           last_error = e
           # Log error for this specific fee tier attempt, but continue to next tier
           logger.warning(f"[{config['name']}] Uniswap: Error getting quote for {token_in_symbol}-{token_out_symbol} at fee {current_fee_tier/10000}%: {e}")
           continue # Try next fee tier
  
   # If we've tried all fee tiers and none worked
   error_msg = f"[{config['name']}] Failed to get Uniswap quote for {token_in_symbol}-{token_out_symbol} after trying all fee tiers: {last_error}"
   logger.error(error_msg)
   raise Exception(error_msg) # This will be caught by the calling function

############################
# DATA PROCESSING FUNCTIONS
############################

def  fetch_uniswap_quote_data (token_a, token_b, notional, cached_gas_price, network_key, config): # 
   """
   Fetch Uniswap quote for a single trading pair and notional amount.
   """
   direction = f"{token_a}->{token_b}"
   uniswap_quote = None
   try:
       uniswap_quote = get_uniswap_quote(token_a, token_b, notional, cached_gas_price, network_key) # 
       if uniswap_quote: # Check if quote was successfully retrieved
            logger.info(f"[{config['name']}] Uniswap: Quote received for {direction} with ${notional} USD")
   except Exception as e:
       # Error logging is handled within get_uniswap_quote if all tiers fail.
       logger.error(f"[{config['name']}] Uniswap: Error getting quote for {direction} with ${notional} USD in fetch_uniswap_quote_data: {e}")
   return uniswap_quote


def  process_trading_pair (pair_data):
   """
   Process a single trading pair with all its notional amounts
   """ # 
   token_a, token_b, config, network_key, cached_gas_price, cycle_timestamp = pair_data
  
   # Skip pairs where tokens don't exist on this network (should not happen with current setup)
   if token_a not in config["tokens"] or token_b not in config["tokens"]: # 
       logger.warning(f"[{config['name']}] Skipping {token_a}/{token_b} - tokens not available on this network")
       return
  
   notionals = USD_NOTIONALS # 
  
   for notional in notionals:
       direction = f"{token_a}->{token_b}"
       logger.info(f"[{cycle_timestamp}] [{config['name']}] Getting Uniswap quote for {direction} with ${notional} USD...")
      
       # Get Uniswap quote
       uniswap_quote = fetch_uniswap_quote_data(token_a, token_b, notional, cached_gas_price, network_key, config)
      
       # Write Uniswap quote to CSV
       if uniswap_quote:
           write_quote_row(uniswap_quote) # 
           logger.info(f"[{config['name']}] Uniswap: Quote written for {direction} with ${notional} USD")
       else: # (adapted)
           logger.warning(f"[{config['name']}] No Uniswap quote available for {direction} with ${notional} USD")

############################
# MAIN FUNCTIONS
############################

def  main ():
   """
   Main execution function for the Uniswap quotes collector
   """
   start_time = time.time()
   logger.info("Starting Uniswap (Ethereum) quote collector...")
   logger.info(f"CSV output file: {CSV_FILE}")
  
   # Log all networks and their trading pairs
   for network_key, config in NETWORK_CONFIGS.items(): 
       logger.info(f"{config['name']} - Tracking pairs: {', '.join([f'{pair[0]}/{pair[1]}' for pair in config['trade_pairs']])}")
  
   logger.info(f"USD notional amounts: {', '.join([f'${n}' for n in USD_NOTIONALS])}") # 
  
   try:
       while True:
           now = dt.datetime.now(dt.UTC).isoformat()
           logger.info(f"[{now}] Starting new quote collection cycle")
          
           # Iterate over each network (preserved for future use)
           for network_key, config in NETWORK_CONFIGS.items():
               logger.info(f"[{now}] Processing {config['name']}...") # 
              
               # Cache the gas price once per network per cycle
               try:
                   cached_gas_price = config["w3"].eth.gas_price
                   logger.info(f"[{config['name']}] Cached gas price for this cycle: {cached_gas_price / 10**9} Gwei") # 
               except Exception as e:
                   logger.error(f"[{config['name']}] Error fetching gas price: {e}")
                   cached_gas_price = None # Allow trades to proceed with on-demand gas price fetching
              
               # Process trading pairs for this network
               if ENABLE_PARALLEL_PROCESSING:
                   pair_tasks = [] # 
                   for token_a, token_b in config["trade_pairs"]:
                       pair_data = (token_a, token_b, config, network_key, cached_gas_price, now)
                       pair_tasks.append(pair_data) # 
                  
                   max_workers = MAX_WORKERS_ETHEREUM
                   logger.info(f"[{config['name']}] Processing {len(pair_tasks)} trading pairs with {max_workers} parallel workers...")
                  
                   with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                       futures = [executor.submit(process_trading_pair, pair_data) for pair_data in pair_tasks] # 
                       for future in concurrent.futures.as_completed(futures): # 
                           try:
                               future.result()
                           except Exception as e: # 
                               # Errors from process_trading_pair should be logged there
                               # This catches errors if process_trading_pair itself fails or re-raises something unexpected.
                               logger.error(f"[{config['name']}] Error processing a trading pair task: {e}")
               else:
                   logger.info(f"[{config['name']}] Processing trading pairs sequentially...") # 
                   for token_a, token_b in config["trade_pairs"]:
                       pair_data = (token_a, token_b, config, network_key, cached_gas_price, now)
                       process_trading_pair(pair_data)

           if not TOGGLE:
               logger.info("TOGGLE is set to False, terminating script after completing one cycle...") # 
               print("TOGGLE is set to False, terminating script after completing one cycle...")
               break
              
           # Sleep some random interval
           sleep_time = random.uniform(.25 * 3600, .5 * 3600) # 
           logger.info(f"Completed cycle. Sleeping for {sleep_time/3600:.2f} hours until next collection...") # 
           time.sleep(sleep_time)
          
   except KeyboardInterrupt:
       logger.info("Keyboard interrupt received. Shutting down quote collector...")
   except Exception as e:
       logger.error(f"Unexpected error in main loop: {e}") # Ensure this gets logged if it's an unhandled one from deeper
       raise
   finally:
       logger.info("Quote collection completed.")
       end_time = time.time()
       total_time = end_time - start_time
       hours = int(total_time // 3600) # 
       minutes = int((total_time % 3600) // 60)
       seconds = int(total_time % 60)
       print(f"Quote collection completed. Total runtime: {hours}h {minutes}m {seconds}s")

if __name__ == "__main__":
   main()

# caffeinate python3 /Users/{}/Documents/crypto/uniswap_quotes.py