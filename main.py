import asyncio
import aiohttp
import csv
import json
from collections import defaultdict
from datetime import datetime

# Configuration
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
API_URL = "https://alpha-biz.sodex.dev/biz/mirror/account_flow"
MAX_CONCURRENT_REQUESTS = 10  # Process 10 addresses at a time to avoid being banned

async def fetch_flow(session, addr, user_totals, daily_totals, overall_totals):
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
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
                return True
    except Exception as e:
        print(f"Error fetching {addr}: {e}")
    return False

async def main():
    print("Fetching registry...")
    async with aiohttp.ClientSession() as session:
        async with session.get(REGISTRY_URL) as resp:
            registry_data = await resp.json()

        user_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        daily_totals = defaultdict(lambda: defaultdict(lambda: {'dep': 0.0, 'with': 0.0}))
        overall_totals = defaultdict(lambda: {'dep': 0.0, 'with': 0.0})

        # Semaphores limit the number of parallel requests to avoid 429 errors
        sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        async def sem_fetch(addr):
            async with sem:
                return await fetch_flow(session, addr, user_totals, daily_totals, overall_totals)

        tasks = [sem_fetch(entry.get('address')) for entry in registry_data if entry.get('address')]
        
        print(f"Starting async sync for {len(tasks)} addresses...")
        await asyncio.gather(*tasks)

        # --- SAVE FILES ---
        # (The CSV saving logic remains the same as before)
        save_csvs(user_totals, daily_totals, overall_totals)
        print("Done!")

def save_csvs(user_totals, daily_totals, overall_totals):
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

if __name__ == "__main__":
    asyncio.run(main())
