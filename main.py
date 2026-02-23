import requests
import csv
from collections import defaultdict
from datetime import datetime
import time

def run_sodex_processor():
    REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
    API_URL = "https://alpha-biz.sodex.dev/biz/mirror/account_flow"
    
    print("Fetching user registry...")
    try:
        reg_res = requests.get(REGISTRY_URL)
        reg_res.raise_for_status()
        registry_data = reg_res.json()
    except Exception as e:
        print(f"Error loading registry: {e}")
        return

    user_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
    daily_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
    overall_totals = defaultdict(lambda: {'dep': 0.0, 'with': 0.0})

    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Origin": "https://sodex.com",
        "Referer": "https://sodex.com/"
    }

    for user_entry in registry_data:
        # Correctly extract the address from the dictionary
        addr = user_entry.get('address')
        if not addr:
            continue
            
        print(f"Syncing: {addr}")
        payload = {"account": addr, "start": 0, "limit": 100}
        
        try:
            res = requests.post(API_URL, json=payload, headers=headers)
            
            # Check if response is valid JSON
            if res.status_code == 200:
                data = res.json()
                if data.get("code") == "0":
                    flows = data.get("data", {}).get("accountFlows", [])
                    for item in flows:
                        token = item.get("coin")
                        decimals = item.get("decimals", 18)
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
                else:
                    print(f"API Error for {addr}: {data.get('message')}")
            else:
                print(f"Server Error for {addr}: Status {res.status_code}")
                
            # Small delay to prevent Cloudflare from blocking GitHub Actions
            time.sleep(0.5)

        except Exception as e:
            print(f"Failed processing {addr}: {e}")

    # --- SAVE CSV FILES ---
    # File 1
    with open('user_token_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['address', 'token', 'total_deposit', 'total_withdrawal'])
        for addr, tokens in user_totals.items():
            for token, vals in tokens.items():
                writer.writerow([addr, token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

    # File 2
    with open('daily_net_flows.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'token', 'total_depo', 'total_with'])
        for date in sorted(daily_totals.keys()):
            for token, vals in daily_totals[date].items():
                writer.writerow([date, token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

    # File 3
    with open('overall_sodex_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['token', 'overall_deposit', 'overall_withdrawal'])
        for token, vals in overall_totals.items():
            writer.writerow([token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

    print("Success: Files updated.")

if __name__ == "__main__":
    run_sodex_processor()
