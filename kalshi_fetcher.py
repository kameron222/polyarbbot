#!/usr/bin/env python3
"""
Kalshi Market Data Fetcher
Fetches live market data from Kalshi API for arbitrage detection
"""

import requests
import json
import csv
import datetime as dt
from typing import List, Dict, Optional
import argparse
import time

class KalshiFetcher:
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initialize fetcher. Public markets are available without auth."""
        self.api_key = api_key
        self.api_secret = api_secret
        # Updated public markets API endpoint
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
    
    def get_markets(self, status: str = "open", limit: int = 500) -> List[Dict]:
        """Fetch markets with cursor pagination from Kalshi public API."""
        url = f"{self.base_url}/markets"
        all_markets: List[Dict] = []
        cursor: Optional[str] = None
        try:
            while True:
                params: Dict[str, object] = {'status': status, 'limit': limit}
                if cursor:
                    params['cursor'] = cursor
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                page = data.get('markets', [])
                if not page:
                    break
                all_markets.extend(page)
                cursor = data.get('cursor')
                if not cursor:
                    break
            return all_markets
        except requests.exceptions.RequestException as e:
            print(f"Error fetching Kalshi markets: {e}")
            return all_markets
        except json.JSONDecodeError as e:
            print(f"Error parsing Kalshi JSON: {e}")
            return all_markets
    
    def normalize_market(self, market: Dict) -> Dict:
        """Normalize Kalshi market data to standard format"""
        # Extract basic info
        title = market.get('title', '').strip()
        description = market.get('description', '').strip()
        
        # Extract end date
        end_date = market.get('close_time', '')
        if end_date:
            try:
                # Convert to ISO format if needed
                end_date = dt.datetime.fromisoformat(end_date.replace('Z', '+00:00')).isoformat()
            except:
                pass
        
        # Convert cents to probability 0..1
        def cents_to_prob(x):
            try:
                return round(float(x) / 100.0, 4)
            except Exception:
                return None

        yes_bid = cents_to_prob(market.get('yes_bid'))
        yes_ask = cents_to_prob(market.get('yes_ask'))
        no_bid = cents_to_prob(market.get('no_bid'))
        no_ask = cents_to_prob(market.get('no_ask'))
        best_bid = yes_bid
        best_ask = yes_ask

        # Outcomes and mid prices
        outcomes = ["Yes", "No"]
        def mid(a, b):
            if a is None or b is None:
                return None
            return round((a + b) / 2.0, 4)
        yes_mid = mid(yes_bid, yes_ask)
        outcome_prices = [p for p in [yes_mid, (1.0 - yes_mid) if yes_mid is not None else None] if p is not None]

        # Get condition ID (ticker)
        condition_id = market.get('ticker', '')

        # Times
        end_date = market.get('close_time') or market.get('latest_expiration_time') or ''
        if end_date:
            try:
                end_date = dt.datetime.fromisoformat(end_date.replace('Z', '+00:00')).isoformat()
            except Exception:
                pass

        # Liquidity and volume (cents -> dollars for liquidity)
        liq_raw = market.get('liquidity', 0)
        try:
            liquidity = round(float(liq_raw) / 100.0, 2)
        except Exception:
            liquidity = 0.0
        volume = market.get('volume', 0)
        
        return {
            'title': title,
            'description': description,
            'endDate': end_date,
            'conditionId': condition_id,
            'outcomes': outcomes,
            'outcomePrices': outcome_prices,
            'bestBid': best_bid,
            'bestAsk': best_ask,
            'liquidity': liquidity,
            'volume': volume,
            'status': market.get('status', ''),
            'open_time': market.get('open_time', ''),
            'close_time': market.get('close_time', ''),
            'latest_expiration_time': market.get('latest_expiration_time', ''),
            'tick_size': market.get('tick_size', 1)
        }
    
    def get_all_open_markets(self) -> List[Dict]:
        """Fetch all open markets from Kalshi"""
        print("Fetching Kalshi markets...")
        
        # Get markets
        markets = self.get_markets(status="open", limit=1000)
        
        if not markets:
            print("No markets found")
            return []
        
        print(f"Found {len(markets)} markets")
        
        # Normalize markets
        normalized_markets = []
        for market in markets:
            try:
                normalized = self.normalize_market(market)
                if normalized['title']:  # Only include markets with titles
                    normalized_markets.append(normalized)
            except Exception as e:
                print(f"Error normalizing market: {e}")
                continue
        
        print(f"Normalized {len(normalized_markets)} markets")
        return normalized_markets
    
    def save_markets(self, markets: List[Dict], json_file: str, csv_file: str):
        """Save markets to JSON and CSV files"""
        # Save JSON
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(markets, f, indent=2, ensure_ascii=False)
        
        # Save CSV
        if markets:
            fieldnames = list(markets[0].keys())
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for market in markets:
                    # Convert lists to pipe-delimited strings for CSV
                    market_copy = market.copy()
                    for key in ['outcomes', 'outcomePrices']:
                        vals = market_copy.get(key)
                        if isinstance(vals, list):
                            market_copy[key] = '|'.join(map(str, vals))
                        elif key not in market_copy:
                            market_copy[key] = ''
                    writer.writerow(market_copy)
        
        print(f"Saved {len(markets)} markets to {json_file} and {csv_file}")

def main():
    parser = argparse.ArgumentParser(description='Fetch Kalshi market data')
    parser.add_argument('--api-key', required=False, help='Kalshi API key (optional)')
    parser.add_argument('--api-secret', required=False, help='Kalshi API secret (optional)')
    parser.add_argument('--json-file', default='kalshi_markets.json', help='Output JSON file')
    parser.add_argument('--csv-file', default='kalshi_markets.csv', help='Output CSV file')
    
    args = parser.parse_args()
    
    # Initialize fetcher
    fetcher = KalshiFetcher(args.api_key, args.api_secret)
    
    # Fetch markets
    markets = fetcher.get_all_open_markets()
    
    if markets:
        # Save markets
        fetcher.save_markets(markets, args.json_file, args.csv_file)
        
        # Print summary
        print(f"\nSummary:")
        print(f"Total markets: {len(markets)}")
        print(f"Markets with prices: {sum(1 for m in markets if m['bestBid'] or m['bestAsk'])}")
        print(f"Markets with liquidity: {sum(1 for m in markets if m['liquidity'] > 0)}")
        
        # Show sample markets
        print(f"\nSample markets:")
        for i, market in enumerate(markets[:3]):
            print(f"{i+1}. {market['title']}")
            print(f"   Ticker: {market['conditionId']}")
            print(f"   End: {market['endDate']}")
            print(f"   Bid/Ask: {market['bestBid']}/{market['bestAsk']}")
            print(f"   Liquidity: {market['liquidity']}")
            print()
    else:
        print("No markets found")

if __name__ == "__main__":
    main()
