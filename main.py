import requests
import csv
from collections import defaultdict
from datetime import datetime

def run_sodex_processor():
    # 1. Configuration - Registry and API
    REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
    API_URL = "https://alpha-biz.sodex.dev/biz/mirror/account_flow"
    
    print("Fetching user registry from GitHub...")
    try:
        reg_res = requests.get(REGISTRY_URL)
        reg_res.raise_for_status()
        user_addresses = reg_res.json()
    except Exception as e:
        print(f"Error loading registry: {e}")
        return

    # Data Accumulators
    # File 1: {address: {token: {'dep': 0.0, 'with': 0.0}}}
    user_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
    # File 2: {date: {token: {'dep': 0.0, 'with': 0.0}}}
    daily_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
    # File 3: {token: {'dep': 0.0, 'with': 0.0}}
    overall_totals = defaultdict(lambda: {'dep': 0.0, 'with': 0.0})

    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

    # 2. Process each address from the registry
    for addr in user_addresses:
        print(f"Syncing transactions for: {addr}")
        payload = {"account": addr, "start": 0, "limit": 100}
        
        try:
            res = requests.post(API_URL, json=payload, headers=headers).json()
            if res.get("code") == "0":
                flows = res.get("data", {}).get("accountFlows", [])
                
                for item in flows:
                    token = item.get("coin")
                    decimals = item.get("decimals", 18)
                    # Convert raw string/int to float based on decimals
                    amount = int(item.get("amount", 0)) / (10 ** decimals)
                    tx_type = item.get("type", "")
                    date_str = datetime.fromtimestamp(item.get("stmp", 0)).strftime('%Y-%m-%d')

                    if "Deposit" in tx_type:
                        user_totals[addr][token]['dep'] += amount
                        daily_totals[date_str][token]['dep'] += amount
                        overall_totals[token]['dep'] += amount
                    elif "Withdraw" in tx_type:
                        user_totals[addr][token]['with'] += amount
                        daily_totals[date_str][token]['with'] += amount
                        overall_totals[token]['with'] += amount
        except Exception as e:
            print(f"Skipping {addr} due to API error: {e}")

    # --- SAVE FILE 1: User Token Totals ---
    with open('user_token_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['address', 'token', 'total_deposit', 'total_withdrawal'])
        for addr, tokens in user_totals.items():
            for token, vals in tokens.items():
                writer.writerow([addr, token, round(vals['dep'], 4), round(vals['with'], 4)])

    # --- SAVE FILE 2: Daily Net Flows (Chronological) ---
    with open('daily_net_flows.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'token', 'total_depo', 'total_with'])
        for date in sorted(daily_totals.keys()):
            for token, vals in daily_totals[date].items():
                writer.writerow([date, token, round(vals['dep'], 4), round(vals['with'], 4)])

    # --- SAVE FILE 3: Overall Sodex Totals ---
    with open('overall_sodex_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['token', 'overall_deposit', 'overall_withdrawal'])
        for token, vals in overall_totals.items():
            writer.writerow([token, round(vals['dep'], 4), round(vals['with'], 4)])

    print("Success: CSV files generated.")

if __name__ == "__main__":
    run_sodex_processor()
