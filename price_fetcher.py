#!/usr/bin/env python3
"""
Live Price Fetcher
Fetches current prices for matched markets every 10 minutes
"""

import json
import requests
import time
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class LivePriceFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PolyArbBot/1.0',
            'Accept': 'application/json'
        })
    
    def fetch_kalshi_market_price(self, market_id: str) -> Optional[Dict]:
        """Fetch current price for a specific Kalshi market"""
        try:
            url = f"https://api.elections.kalshi.com/trade-api/v2/markets/{market_id}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                market = data.get('market', {})
                
                return {
                    'market_id': market_id,
                    'yes_bid': market.get('yes_bid', 0) / 100.0 if market.get('yes_bid') else None,
                    'yes_ask': market.get('yes_ask', 0) / 100.0 if market.get('yes_ask') else None,
                    'no_bid': market.get('no_bid', 0) / 100.0 if market.get('no_bid') else None,
                    'no_ask': market.get('no_ask', 0) / 100.0 if market.get('no_ask') else None,
                    'last_updated': time.time()
                }
            else:
                logger.warning(f"Kalshi API error for {market_id}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching Kalshi price for {market_id}: {e}")
            return None
    
    def fetch_polymarket_price(self, market_id: str) -> Optional[Dict]:
        """Fetch current price for a specific Polymarket market"""
        try:
            # Try the Gamma API first
            url = f"https://gamma-api.polymarket.com/markets/{market_id}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                market = response.json()
                outcome_prices = market.get('outcomePrices', [])
                
                if isinstance(outcome_prices, str):
                    try:
                        outcome_prices = json.loads(outcome_prices)
                    except:
                        outcome_prices = []
                
                if len(outcome_prices) >= 2:
                    yes_price = float(outcome_prices[0]) if outcome_prices[0] else None
                    no_price = float(outcome_prices[1]) if outcome_prices[1] else None
                    
                    # Convert to 0-1 if needed
                    if yes_price and yes_price > 1:
                        yes_price = yes_price / 100.0
                    if no_price and no_price > 1:
                        no_price = no_price / 100.0
                    
                    return {
                        'market_id': market_id,
                        'yes_price': yes_price,
                        'no_price': no_price,
                        'last_updated': time.time()
                    }
            else:
                logger.warning(f"Polymarket API error for {market_id}: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching Polymarket price for {market_id}: {e}")
            return None
    
    def fetch_live_prices(self, matches: List[Dict]) -> Dict[str, Dict]:
        """Fetch live prices for all matched markets"""
        logger.info(f"Fetching live prices for {len(matches)} market pairs...")
        
        live_prices = {
            'kalshi': {},
            'polymarket': {},
            'timestamp': time.time()
        }
        
        # Extract unique market IDs
        kalshi_ids = set()
        poly_ids = set()
        
        for match in matches:
            kalshi_id = match.get('kalshi_id')
            poly_id = match.get('poly_id')
            
            if kalshi_id:
                kalshi_ids.add(kalshi_id)
            if poly_id:
                poly_ids.add(poly_id)
        
        logger.info(f"Fetching prices for {len(kalshi_ids)} Kalshi + {len(poly_ids)} Polymarket markets")
        
        # Fetch Kalshi prices
        for i, kalshi_id in enumerate(kalshi_ids):
            if i % 10 == 0:
                logger.info(f"  Kalshi progress: {i}/{len(kalshi_ids)}")
            
            price_data = self.fetch_kalshi_market_price(kalshi_id)
            if price_data:
                live_prices['kalshi'][kalshi_id] = price_data
            
            time.sleep(0.1)  # Rate limiting
        
        # Fetch Polymarket prices
        for i, poly_id in enumerate(poly_ids):
            if i % 10 == 0:
                logger.info(f"  Polymarket progress: {i}/{len(poly_ids)}")
            
            price_data = self.fetch_polymarket_price(poly_id)
            if price_data:
                live_prices['polymarket'][poly_id] = price_data
            
            time.sleep(0.1)  # Rate limiting
        
        logger.info(f"✅ Fetched {len(live_prices['kalshi'])} Kalshi + {len(live_prices['polymarket'])} Polymarket prices")
        
        return live_prices
    
    def save_live_prices(self, live_prices: Dict, filename: str = "live_prices.json"):
        """Save live prices to JSON file"""
        try:
            with open(filename, 'w') as f:
                json.dump(live_prices, f, indent=2)
            logger.info(f"💾 Saved live prices to {filename}")
        except Exception as e:
            logger.error(f"Error saving live prices: {e}")


def main():
    """Standalone price fetcher for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch live prices for matched markets')
    parser.add_argument('--matches', default='strict_matches.json', help='Path to matches file')
    parser.add_argument('--output', default='live_prices.json', help='Output file for live prices')
    
    args = parser.parse_args()
    
    # Load matches
    try:
        with open(args.matches) as f:
            data = json.load(f)
        matches = data.get('matches', [])
        print(f"Loaded {len(matches)} market matches")
    except Exception as e:
        print(f"Error loading matches: {e}")
        return
    
    # Fetch live prices
    fetcher = LivePriceFetcher()
    live_prices = fetcher.fetch_live_prices(matches)
    
    # Save results
    fetcher.save_live_prices(live_prices, args.output)
    
    print(f"✅ Live price fetch complete!")
    print(f"📊 Kalshi markets: {len(live_prices['kalshi'])}")
    print(f"📊 Polymarket markets: {len(live_prices['polymarket'])}")


if __name__ == "__main__":
    main()
