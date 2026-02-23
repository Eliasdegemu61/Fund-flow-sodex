import asyncio
import aiohttp
import csv
import json
from collections import defaultdict
from datetime import datetime

# 1. Configuration
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
API_URL = "https://alpha-biz.sodex.dev/biz/mirror/account_flow"
MAX_CONCURRENT_REQUESTS = 15  # Speeds up processing while staying safe from bans

async def fetch_flow(session, addr, user_totals, daily_totals, overall_totals):
    """Fetches flow data for a single address and updates accumulators."""
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://sodex.com",
        "Referer": "https://sodex.com/"
    }
    payload = {"account": addr, "start": 0, "limit": 100}
    
    try:
        async with session.post(API_URL, json=payload, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                if data.get("code") == "0":
                    flows = data.get("data", {}).get("accountFlows", [])
                    for item in flows:
                        token = item.get("coin")
                        decimals = item.get("decimals", 18)
                        # Avoid floating point errors by using float conversion
                        amount = int(item.get("amount", 0)) / (10 ** decimals)
                        tx_type = item.get("type", "")
                        # Convert unix timestamp to YYYY-MM-DD
                        date_str = datetime.fromtimestamp(item.get("stmp", 0)).strftime('%Y-%m-%d')

                        if "Deposit" in tx_type:
                            user_totals[addr][token]['dep'] += amount
                            daily_totals[date_str][token]['dep'] += amount
                            overall_totals[token]['dep'] += amount
                        elif "Withdraw" in tx_type:
                            user_totals[addr][token]['with'] += amount
                            daily_totals[date_str][token]['with'] += amount
                            overall_totals[token]['with'] += amount
                return True
    except Exception as e:
        print(f"Error fetching {addr}: {e}")
    return False

def save_csvs(user_totals, daily_totals, overall_totals):
    """Writes processed data into the three requested CSV files."""
    
    # File 1: address, token, total_deposit, total_withdrawal
    with open('user_token_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['address', 'token', 'total_deposit', 'total_withdrawal'])
        for addr, tokens in user_totals.items():
            for token, vals in tokens.items():
                writer.writerow([addr, token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

    # File 2: date, token, total_depo, total_with (For Charting)
    with open('daily_net_flows.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'token', 'total_depo', 'total_with'])
        for date in sorted(daily_totals.keys()):
            for token, vals in daily_totals[date].items():
                writer.writerow([date, token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

    # File 3: token, overall_deposit, overall_withdrawal
    with open('overall_sodex_totals.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['token', 'overall_deposit', 'overall_withdrawal'])
        for token, vals in overall_totals.items():
            writer.writerow([token, f"{vals['dep']:.4f}", f"{vals['with']:.4f}"])

async def main():
    print("Fetching registry...")
    async with aiohttp.ClientSession() as session:
        # content_type=None fixes the aiohttp error with GitHub's text/plain headers
        async with session.get(REGISTRY_URL) as resp:
            registry_data = await resp.json(content_type=None)

        # Initialize Accumulators
        user_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        daily_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        overall_totals = defaultdict(lambda: {'dep': 0.0, 'with': 0.0})

        # Semaphore limits the number of parallel requests to avoid triggering Cloudflare
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def sem_fetch(addr):
            async with sem:
                return await fetch_flow(session, addr, user_totals, daily_totals, overall_totals)

        # Extract address from the dictionary list in registry.json
        tasks = [sem_fetch(entry.get('address')) for entry in registry_data if entry.get('address')]
        
        print(f"Starting rapid sync for {len(tasks)} addresses...")
        await asyncio.gather(*tasks)

        print("Finalizing data and saving CSV files...")
        save_csvs(user_totals, daily_totals, overall_totals)
        print("✅ Done! Files generated: user_token_totals.csv, daily_net_flows.csv, overall_sodex_totals.csv")

if __name__ == "__main__":
    asyncio.run(main())
