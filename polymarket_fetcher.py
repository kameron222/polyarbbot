#!/usr/bin/env python3
"""
Polymarket Arbitrage Bot - Market Data Fetcher
Gets current active markets from Polymarket for arbitrage analysis.
"""

import requests
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone


class PolymarketFetcher:
    """Class to fetch current active markets from Polymarket for arbitrage analysis."""
    
    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PolyArbBot/1.0',
            'Accept': 'application/json'
        })
    
    def get_current_markets(self, limit: int = 1000) -> List[Dict]:
        """
        Get current active markets that are not closed.
        
        Args:
            limit: Maximum number of markets to fetch
            
        Returns:
            List of current market dictionaries
        """
        try:
            url = f"{self.base_url}/markets"
            params = {
                'active': True,
                'closed': False,
                'limit': limit
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching current markets: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            return []
    
    def get_all_markets(self, limit: int = 1000) -> List[Dict]:
        """
        Get all markets for comparison.
        
        Args:
            limit: Maximum number of markets to fetch
            
        Returns:
            List of all market dictionaries
        """
        try:
            url = f"{self.base_url}/markets"
            params = {'limit': limit}
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching all markets: {e}")
            return []

    def get_all_open_markets(self, page_limit: int = 500, end_date_min: Optional[str] = None) -> List[Dict]:
        """
        Fetch ALL open markets using Gamma API pagination.

        Args:
            page_limit: Number of markets per request (Gamma max is 500)
            end_date_min: Optional ISO date string (e.g., "2025-01-01T00:00:00Z") to filter to future markets server-side

        Returns:
            List of open (closed=false) market dictionaries across all pages
        """
        all_markets: List[Dict] = []
        offset: int = 0

        while True:
            try:
                url = f"{self.base_url}/markets"
                params: Dict[str, object] = {
                    'closed': 'false',
                    'limit': page_limit,
                    'offset': offset,
                }
                if end_date_min:
                    params['end_date_min'] = end_date_min

                resp = self.session.get(url, params=params, timeout=20)
                resp.raise_for_status()
                page = resp.json()

                if not page:
                    break

                all_markets.extend(page)
                offset += page_limit

            except requests.exceptions.RequestException as e:
                print(f"Error fetching open markets (offset={offset}): {e}")
                break
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON (offset={offset}): {e}")
                break

        return all_markets

    def filter_future_markets(self, markets: List[Dict]) -> List[Dict]:
        """Return markets with endDate strictly in the future (UTC)."""
        future: List[Dict] = []
        now = datetime.now(timezone.utc)
        for m in markets:
            end_str = m.get('endDate')
            if not end_str:
                continue
            try:
                end_dt = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
            except Exception:
                continue
            if end_dt > now:
                future.append(m)
        return future
    
    def format_market_data(self, markets: List[Dict]) -> None:
        """
        Print formatted market data.
        
        Args:
            markets: List of market dictionaries
        """
        if not markets:
            print("No markets found.")
            return
        
        print(f"\n{'='*100}")
        print(f"CURRENT ACTIVE MARKETS ({len(markets)} found)")
        print(f"{'='*100}")
        
        for i, market in enumerate(markets, 1):
            print(f"\n{i}. {market.get('question', 'N/A')}")
            print(f"   Market ID: {market.get('id', 'N/A')}")
            print(f"   Condition ID: {market.get('conditionId', 'N/A')}")
            print(f"   Active: {market.get('active', 'N/A')}")
            print(f"   Closed: {market.get('closed', 'N/A')}")
            print(f"   Archived: {market.get('archived', 'N/A')}")
            print(f"   End Date: {market.get('endDate', 'N/A')}")
            
            # Handle volume and liquidity
            volume = market.get('volume', 0)
            liquidity = market.get('liquidity', 0)
            try:
                volume_float = float(volume) if volume else 0
                liquidity_float = float(liquidity) if liquidity else 0
                print(f"   Volume: ${volume_float:,.2f}")
                print(f"   Liquidity: ${liquidity_float:,.2f}")
            except:
                print(f"   Volume: {volume}")
                print(f"   Liquidity: {liquidity}")
            
            # Show outcome prices if available
            outcomes = market.get('outcomes', [])
            outcome_prices = market.get('outcomePrices', [])
            
            if outcomes and outcome_prices and len(outcomes) == len(outcome_prices):
                print("   Outcomes:")
                for outcome, price in zip(outcomes, outcome_prices):
                    try:
                        price_float = float(price)
                        print(f"     - {outcome}: ${price_float:.4f}")
                    except:
                        print(f"     - {outcome}: {price}")
            
            print("-" * 80)
    
    def save_to_files(self, markets: List[Dict], filename_prefix: str = "current_markets") -> None:
        """
        Save markets to JSON and CSV files.
        
        Args:
            markets: List of market dictionaries
            filename_prefix: Prefix for the output files
        """
        if not markets:
            print("No markets to save.")
            return
        
        # Save JSON
        json_filename = f"{filename_prefix}.json"
        with open(json_filename, 'w') as f:
            json.dump(markets, f, indent=2)
        print(f"Saved {len(markets)} markets to {json_filename}")
        
        # Save CSV
        csv_filename = f"{filename_prefix}.csv"
        with open(csv_filename, 'w') as f:
            f.write("Question,Market_ID,Condition_ID,Active,Closed,Archived,End_Date,Volume,Liquidity,Outcomes\n")
            
            for market in markets:
                question = market.get('question', '').replace('"', '""')
                market_id = market.get('id', '')
                condition_id = market.get('conditionId', '')
                active = market.get('active', False)
                closed = market.get('closed', False)
                archived = market.get('archived', False)
                end_date = market.get('endDate', '')
                volume = market.get('volume', 0)
                liquidity = market.get('liquidity', 0)
                
                # Format outcomes
                outcomes = market.get('outcomes', [])
                outcome_prices = market.get('outcomePrices', [])
                outcomes_str = ""
                if outcomes and outcome_prices and len(outcomes) == len(outcome_prices):
                    outcome_list = []
                    for outcome, price in zip(outcomes, outcome_prices):
                        outcome_list.append(f"{outcome}:{price}")
                    outcomes_str = "|".join(outcome_list)
                
                f.write(f'"{question}",{market_id},{condition_id},{active},{closed},{archived},{end_date},{volume},{liquidity},"{outcomes_str}"\n')
        
        print(f"Saved CSV to {csv_filename}")
    
    def analyze_markets(self, markets: List[Dict]) -> None:
        """
        Analyze and display statistics about the markets.
        
        Args:
            markets: List of market dictionaries
        """
        if not markets:
            print("No markets to analyze.")
            return
        
        print(f"\n{'='*100}")
        print("MARKET ANALYSIS:")
        print(f"{'='*100}")
        
        # Count by end date year
        year_counts = {}
        for market in markets:
            end_date = market.get('endDate', '')
            if end_date:
                year = end_date[:4]
                year_counts[year] = year_counts.get(year, 0) + 1
        
        print("Markets by end date year:")
        for year in sorted(year_counts.keys()):
            print(f"  {year}: {year_counts[year]} markets")
        
        # Count by category
        category_counts = {}
        for market in markets:
            category = market.get('category', 'Unknown')
            category_counts[category] = category_counts.get(category, 0) + 1
        
        print("\nMarkets by category:")
        for category, count in sorted(category_counts.items()):
            print(f"  {category}: {count} markets")
        
        # Show markets ending soon (next 30 days)
        current_time = datetime.now(timezone.utc)
        soon_markets = []
        
        for market in markets:
            end_date_str = market.get('endDate', '')
            if end_date_str:
                try:
                    end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                    days_until_end = (end_date - current_time).days
                    if 0 <= days_until_end <= 30:
                        soon_markets.append((market, days_until_end))
                except:
                    continue
        
        if soon_markets:
            print(f"\nMarkets ending in the next 30 days ({len(soon_markets)}):")
            for market, days in sorted(soon_markets, key=lambda x: x[1]):
                print(f"  {days} days: {market.get('question', 'N/A')}")
        else:
            print(f"\nNo markets ending in the next 30 days.")


def main():
    """Main function to fetch current Polymarket data for arbitrage analysis."""
    print("Polymarket Arbitrage Bot - Market Data Fetcher")
    print("=" * 50)
    
    api = PolymarketFetcher()
    
    # Get ALL open markets with pagination (target ~1200+)
    print("Fetching all open markets (closed=false) with pagination...")
    all_open = api.get_all_open_markets(page_limit=500)
    
    if all_open:
        print(f"Found {len(all_open)} open markets (closed=false)!")
        
        # Save to files
        api.save_to_files(all_open, "polymarket_current_active_gamma")
        
        # Display markets
        api.format_market_data(all_open[:500])
        
        # Analyze markets
        # Filter to future end dates
        future_only = api.filter_future_markets(all_open)
        print(f"\nFuture-dated markets: {len(future_only)}")
        api.analyze_markets(future_only)
        
        print(f"\n{'='*100}")
        print("SUCCESS! Found current active markets for arbitrage analysis.")
        print("=" * 100)
        
    else:
        print("No current active markets found.")
        
        # Try to get all markets for comparison
        print("\nFetching all markets for comparison...")
        all_markets = api.get_all_markets(limit=100)
        
        if all_markets:
            print(f"Found {len(all_markets)} total markets")
            
            # Show some examples
            print("\nFirst 5 markets:")
            for i, market in enumerate(all_markets[:5], 1):
                print(f"{i}. {market.get('question', 'N/A')} (Active: {market.get('active', 'N/A')}, Closed: {market.get('closed', 'N/A')})")


if __name__ == "__main__":
    main()
