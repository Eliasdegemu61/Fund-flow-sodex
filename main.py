import asyncio
import aiohttp
import csv
import json
from collections import defaultdict
from datetime import datetime

# 1. Configuration
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
API_URL = "https://alpha-biz.sodex.dev/biz/mirror/account_flow"
MAX_CONCURRENT_REQUESTS = 15 

async def fetch_flow(session, addr, user_totals, daily_totals, overall_totals):
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://sodex.com",
        "Referer": "https://sodex.com/"
    }
    
    start = 0
    limit = 100 # The API caps at 100 per request
    
    try:
        while True:
            payload = {"account": addr, "start": start, "limit": limit}
            async with session.post(API_URL, json=payload, headers=headers) as response:
                if response.status != 200:
                    break
                
                res_json = await response.json()
                data_section = res_json.get("data")
                
                # If 'data' is null or missing, we've hit the end
                if not data_section:
                    break
                
                flows = data_section.get("accountFlows", [])
                if not flows:
                    break
                
                for item in flows:
                    token = item.get("coin", "UNKNOWN").upper()
                    # Default to 18 if decimals missing, but USDC will use 6
                    decimals = int(item.get("decimals", 18))
                    
                    try:
                        # Convert raw amount to human readable
                        amount = int(item.get("amount", 0)) / (10 ** decimals)
                    except:
                        continue
                        
                    tx_type = item.get("type", "")
                    # Convert timestamp to YYYY-MM-DD
                    date_str = datetime.fromtimestamp(item.get("stmp", 0)).strftime('%Y-%m-%d')

                    # Logic: Aggregate totals based on transaction type
                    if "Deposit" in tx_type:
                        user_totals[addr][token]['dep'] += amount
                        daily_totals[date_str][token]['dep'] += amount
                        overall_totals[token]['dep'] += amount
                    elif "Withdraw" in tx_type:
                        user_totals[addr][token]['with'] += amount
                        daily_totals[date_str][token]['with'] += amount
                        overall_totals[token]['with'] += amount
                
                # If we retrieved fewer than the limit, there are no more pages
                if len(flows) < limit:
                    break
                
                # Increment offset to fetch the next 100 transactions
                start += limit
                
        return True
    except Exception as e:
        print(f"Error fetching {addr}: {e}")
        return False

def save_csvs(user_totals, daily_totals, overall_totals):
    # File 1: User Summary (One row per address-token pair)
    with open('user_token_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['address', 'token', 'total_deposit', 'total_withdrawal'])
        for addr in sorted(user_totals.keys()):
            for token in sorted(user_totals[addr].keys()):
                vals = user_totals[addr][token]
                if vals['dep'] > 0 or vals['with'] > 0:
                    writer.writerow([addr, token, f"{vals['dep']:.6f}", f"{vals['with']:.6f}"])

    # File 2: Daily Net (For Charting)
    with open('daily_net_flows.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'token', 'total_depo', 'total_with'])
        for date in sorted(daily_totals.keys()):
            for token in sorted(daily_totals[date].keys()):
                vals = daily_totals[date][token]
                writer.writerow([date, token, f"{vals['dep']:.6f}", f"{vals['with']:.6f}"])

    # File 3: Overall totals
    with open('overall_sodex_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['token', 'overall_deposit', 'overall_withdrawal'])
        for token in sorted(overall_totals.keys()):
            vals = overall_totals[token]
            writer.writerow([token, f"{vals['dep']:.6f}", f"{vals['with']:.6f}"])

async def main():
    print("Starting processing with Pagination...")
    async with aiohttp.ClientSession() as session:
        # Fetching registry from your GitHub
        async with session.get(REGISTRY_URL) as resp:
            registry_data = await resp.json(content_type=None)

        # Storage structures
        user_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        daily_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        overall_totals = defaultdict(lambda: {'dep': 0.0, 'with': 0.0})

        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def sem_fetch(addr):
            async with sem:
                return await fetch_flow(session, addr, user_totals, daily_totals, overall_totals)

        # Start the batch request for all addresses in registry
        tasks = [sem_fetch(entry.get('address')) for entry in registry_data if entry.get('address')]
        print(f"Syncing {len(tasks)} addresses (Full History via Pagination)...")
        await asyncio.gather(*tasks)

        print("Generating CSV files...")
        save_csvs(user_totals, daily_totals, overall_totals)
        print("✅ Done!")

if __name__ == "__main__":
    asyncio.run(main())
