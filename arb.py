#!/usr/bin/env python3
"""
Arbitrage Scanner
Scans matched markets for arbitrage opportunities and broadcasts to Discord
"""

import json
import requests
import datetime as dt
from typing import Dict, List, Optional, Tuple
import time
import argparse

# Discord webhook URL - set via environment variable or command line
DISCORD_WEBHOOK = "YOUR_DISCORD_WEBHOOK_URL_HERE"


class ArbScanner:
    def __init__(self, webhook_url: str = DISCORD_WEBHOOK):
        self.webhook_url = webhook_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'PolyArbBot/1.0'
        })
    
    def load_matches(self, matches_file: str) -> List[Dict]:
        """Load market matches from JSON file"""
        try:
            with open(matches_file, 'r') as f:
                data = json.load(f)
            matches = data.get('matches', [])
            print(f"Loaded {len(matches)} market matches from {matches_file}")
            return matches
        except Exception as e:
            print(f"Error loading matches: {e}")
            return []
    
    def load_live_prices(self, live_prices_file: str) -> Optional[Dict]:
        """Load live prices from JSON file"""
        try:
            with open(live_prices_file, 'r') as f:
                data = json.load(f)
            print(f"Loaded live prices: {len(data.get('kalshi', {}))} Kalshi + {len(data.get('polymarket', {}))} Polymarket")
            return data
        except Exception as e:
            print(f"Error loading live prices: {e}")
            return None
    
    def load_market_data(self, kalshi_file: str, poly_file: str) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
        """Load current market data and index by ID"""
        print("Loading current market data...")
        
        # Load Kalshi markets
        kalshi_markets = {}
        try:
            with open(kalshi_file, 'r') as f:
                kalshi_data = json.load(f)
            for market in kalshi_data:
                market_id = market.get('conditionId')
                if market_id:
                    kalshi_markets[market_id] = market
            print(f"Loaded {len(kalshi_markets)} Kalshi markets")
        except Exception as e:
            print(f"Error loading Kalshi data: {e}")
        
        # Load Polymarket markets
        poly_markets = {}
        try:
            with open(poly_file, 'r') as f:
                poly_data = json.load(f)
            for market in poly_data:
                market_id = market.get('id')
                if market_id:
                    poly_markets[market_id] = market
            print(f"Loaded {len(poly_markets)} Polymarket markets")
        except Exception as e:
            print(f"Error loading Polymarket data: {e}")
        
        return kalshi_markets, poly_markets
    
    def parse_kalshi_prices(self, market: Dict) -> Optional[Dict]:
        """Extract prices from Kalshi market"""
        try:
            # Get bid/ask prices (in probability 0-1)
            bid = market.get('bestBid')
            ask = market.get('bestAsk')
            
            # Convert from cents to probability if needed
            if bid is not None and bid > 1:
                bid = bid / 100.0
            if ask is not None and ask > 1:
                ask = ask / 100.0
            
            return {
                'yes_bid': bid,
                'yes_ask': ask,
                'no_bid': (1 - ask) if ask is not None else None,
                'no_ask': (1 - bid) if bid is not None else None,
                'mid': (bid + ask) / 2 if (bid is not None and ask is not None) else None
            }
        except Exception as e:
            print(f"Error parsing Kalshi prices: {e}")
            return None
    
    def parse_polymarket_prices(self, market: Dict) -> Optional[Dict]:
        """Extract prices from Polymarket market"""
        try:
            # Get outcome prices
            outcome_prices = market.get('outcomePrices', [])
            if isinstance(outcome_prices, str):
                try:
                    outcome_prices = json.loads(outcome_prices)
                except:
                    outcome_prices = []
            
            if not outcome_prices or len(outcome_prices) < 2:
                return None
            
            # Assume binary market: [Yes, No] prices
            yes_price = float(outcome_prices[0]) if outcome_prices[0] else None
            no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 and outcome_prices[1] else None
            
            # Convert to 0-1 probability if needed
            if yes_price is not None and yes_price > 1:
                yes_price = yes_price / 100.0
            if no_price is not None and no_price > 1:
                no_price = no_price / 100.0
            
            return {
                'yes_price': yes_price,
                'no_price': no_price,
                'mid': yes_price
            }
        except Exception as e:
            print(f"Error parsing Polymarket prices: {e}")
            return None
    
    def calculate_arbitrage(self, kalshi_prices: Dict, poly_prices: Dict, match: Dict) -> Optional[Dict]:
        """Calculate arbitrage opportunity using complementary positions (YES + NO = 1.00)"""
        try:
            # Kalshi prices
            k_yes_bid = kalshi_prices.get('yes_bid')
            k_yes_ask = kalshi_prices.get('yes_ask')
            k_no_bid = kalshi_prices.get('no_bid') 
            k_no_ask = kalshi_prices.get('no_ask')
            
            # Polymarket prices
            p_yes_price = poly_prices.get('yes_price')
            p_no_price = poly_prices.get('no_price')
            
            if not all([k_yes_ask, k_no_ask, p_yes_price]):
                return None
            
            # Platform fees (worst case assumptions)
            kalshi_fee = 0.02  # 2% on winning side
            poly_fee = 0.005   # 0.5% (gas + potential fees)
            
            opportunities = []
            
            # Strategy 1: Buy Polymarket YES + Buy Kalshi NO
            # This works when: poly_yes_price + kalshi_no_ask < 1.00 (after fees)
            if p_yes_price and k_no_ask:
                cost = p_yes_price + k_no_ask
                
                # Calculate expected payout after fees
                # If YES wins: Poly pays $1 - poly_fee, Kalshi NO loses
                # If NO wins: Kalshi pays $1 - kalshi_fee, Poly YES loses
                expected_payout_yes = (1 - poly_fee) + 0  # Poly wins, Kalshi loses
                expected_payout_no = 0 + (1 - kalshi_fee)   # Kalshi wins, Poly loses
                
                # Use worst case (minimum payout)
                min_payout = min(expected_payout_yes, expected_payout_no)
                
                if cost < min_payout:
                    profit = min_payout - cost
                    profit_pct = (profit / cost) * 100
                    
                    opportunities.append({
                        'type': 'poly_yes_kalshi_no',
                        'action': f'Buy Polymarket YES @ {p_yes_price:.3f} + Buy Kalshi NO @ {k_no_ask:.3f}',
                        'cost': cost,
                        'min_payout': min_payout,
                        'profit': profit,
                        'profit_pct': profit_pct,
                        'poly_price': p_yes_price,
                        'kalshi_price': k_no_ask,
                        'strategy': 'Complementary positions'
                    })
            
            # Strategy 2: Buy Polymarket NO + Buy Kalshi YES  
            if p_no_price and k_yes_ask:
                cost = p_no_price + k_yes_ask
                
                # Calculate expected payout after fees
                expected_payout_yes = 0 + (1 - kalshi_fee)   # Kalshi wins, Poly loses
                expected_payout_no = (1 - poly_fee) + 0      # Poly wins, Kalshi loses
                
                min_payout = min(expected_payout_yes, expected_payout_no)
                
                if cost < min_payout:
                    profit = min_payout - cost
                    profit_pct = (profit / cost) * 100
                    
                    opportunities.append({
                        'type': 'poly_no_kalshi_yes',
                        'action': f'Buy Polymarket NO @ {p_no_price:.3f} + Buy Kalshi YES @ {k_yes_ask:.3f}',
                        'cost': cost,
                        'min_payout': min_payout,
                        'profit': profit,
                        'profit_pct': profit_pct,
                        'poly_price': p_no_price,
                        'kalshi_price': k_yes_ask,
                        'strategy': 'Complementary positions'
                    })
            
            # Strategy 3: Traditional same-side arbitrage (higher risk)
            # Buy low, sell high on same outcome
            if k_yes_ask and k_yes_bid and p_yes_price:
                # Buy Kalshi YES, Sell Polymarket YES
                if k_yes_ask < p_yes_price * (1 - poly_fee):
                    net_poly_receive = p_yes_price * (1 - poly_fee)
                    kalshi_cost = k_yes_ask
                    if net_poly_receive > kalshi_cost:
                        profit = net_poly_receive - kalshi_cost
                        profit_pct = (profit / kalshi_cost) * 100
                        
                        opportunities.append({
                            'type': 'same_side_yes',
                            'action': f'Buy Kalshi YES @ {k_yes_ask:.3f}, Sell Polymarket YES @ {p_yes_price:.3f}',
                            'cost': kalshi_cost,
                            'min_payout': net_poly_receive,
                            'profit': profit,
                            'profit_pct': profit_pct,
                            'poly_price': p_yes_price,
                            'kalshi_price': k_yes_ask,
                            'strategy': 'Same-side arbitrage (risky)'
                        })
            
            # Return best opportunity
            if opportunities:
                best_opp = max(opportunities, key=lambda x: x['profit_pct'])
                if best_opp['profit_pct'] > 0.5:  # Only report if >0.5% profit after fees
                    return best_opp
            
            return None
            
        except Exception as e:
            print(f"Error calculating arbitrage: {e}")
            return None
    
    def format_discord_message(self, arb: Dict, match: Dict) -> Dict:
        """Format arbitrage opportunity as Discord embed"""
        
        # Color based on strategy type
        if arb.get('strategy') == 'Complementary positions':
            color = 0x00FF00  # Green for safe arbitrage
        else:
            color = 0xFF8C00  # Orange for risky arbitrage
        
        # Format cost and payout
        cost = arb.get('cost', 0)
        min_payout = arb.get('min_payout', 0)
        profit = arb.get('profit', 0)
        
        embed = {
            "title": "🚨 ARBITRAGE OPPORTUNITY 🚨",
            "color": color,
            "fields": [
                {
                    "name": "📊 Market",
                    "value": f"**Kalshi:** {match['kalshi_title'][:80]}...\n**Polymarket:** {match['poly_title'][:80]}...",
                    "inline": False
                },
                {
                    "name": "💰 Strategy",
                    "value": f"**{arb.get('strategy', 'Unknown')}**\n{arb['action']}",
                    "inline": False
                },
                {
                    "name": "💵 Cost",
                    "value": f"${cost:.3f}",
                    "inline": True
                },
                {
                    "name": "💸 Min Payout",
                    "value": f"${min_payout:.3f}",
                    "inline": True
                },
                {
                    "name": "📈 Profit",
                    "value": f"**${profit:.3f} ({arb['profit_pct']:.2f}%)**",
                    "inline": True
                },
                {
                    "name": "🏷️ Domain",
                    "value": match.get('domain', 'unknown').title(),
                    "inline": True
                },
                {
                    "name": "⭐ Match Score",
                    "value": f"{match.get('score', 0):.1f}",
                    "inline": True
                },
                {
                    "name": "⚠️ Fees Included",
                    "value": "Kalshi: 2% | Poly: 0.5%",
                    "inline": True
                }
            ],
            "footer": {
                "text": f"PolyArbBot • {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            }
        }
        
        return {
            "embeds": [embed]
        }
    
    def send_discord_alert(self, message: Dict) -> bool:
        """Send alert to Discord webhook"""
        try:
            response = self.session.post(self.webhook_url, json=message, timeout=10)
            if response.status_code == 204:
                print("✅ Discord alert sent successfully")
                return True
            else:
                print(f"❌ Discord alert failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error sending Discord alert: {e}")
            return False
    
    def scan_for_arbitrage_with_live_prices(self, matches: List[Dict], live_prices: Dict,
                          min_profit_pct: float = 0.5, max_profit_pct: float = 25.0, max_alerts: int = 10) -> List[Dict]:
        """Scan for arbitrage using live prices"""
        
        print(f"Scanning {len(matches)} matched markets for arbitrage opportunities...")
        print(f"Profit threshold: {min_profit_pct}% - {max_profit_pct}% (filtering out unrealistic opportunities)")
        
        kalshi_prices = live_prices.get('kalshi', {})
        poly_prices = live_prices.get('polymarket', {})
        
        opportunities = []
        alerts_sent = 0
        
        for i, match in enumerate(matches):
            if alerts_sent >= max_alerts:
                print(f"Reached maximum alert limit ({max_alerts})")
                break
                
            if i % 20 == 0:
                print(f"  Processed {i}/{len(matches)} matches...")
            
            # Get live prices
            kalshi_id = match.get('kalshi_id')
            poly_id = match.get('poly_id')
            
            if not kalshi_id or not poly_id:
                continue
            
            kalshi_live = kalshi_prices.get(kalshi_id)
            poly_live = poly_prices.get(poly_id)
            
            if not kalshi_live or not poly_live:
                continue
            
            # Convert to expected format
            kalshi_price_data = {
                'yes_bid': kalshi_live.get('yes_bid'),
                'yes_ask': kalshi_live.get('yes_ask'),
                'no_bid': kalshi_live.get('no_bid'),
                'no_ask': kalshi_live.get('no_ask')
            }
            
            poly_price_data = {
                'yes_price': poly_live.get('yes_price'),
                'no_price': poly_live.get('no_price')
            }
            
            # Calculate arbitrage
            arb = self.calculate_arbitrage(kalshi_price_data, poly_price_data, match)
            
            if arb and min_profit_pct <= arb['profit_pct'] <= max_profit_pct:
                print(f"\n🚨 ARBITRAGE FOUND:")
                print(f"   Market: {match['kalshi_title'][:60]}...")
                print(f"   Strategy: {arb.get('strategy', 'Unknown')}")
                print(f"   Action: {arb['action']}")
                print(f"   Cost: ${arb.get('cost', 0):.3f}")
                print(f"   Min Payout: ${arb.get('min_payout', 0):.3f}")
                print(f"   Profit: ${arb.get('profit', 0):.3f} ({arb['profit_pct']:.2f}%)")
                
                opportunities.append({
                    'match': match,
                    'arbitrage': arb,
                    'kalshi_prices': kalshi_live,
                    'poly_prices': poly_live
                })
                
                # Send Discord alert
                discord_msg = self.format_discord_message(arb, match)
                if self.send_discord_alert(discord_msg):
                    alerts_sent += 1
                    time.sleep(1)  # Rate limit protection
            
            elif arb and arb['profit_pct'] > max_profit_pct:
                print(f"   ⚠️ Filtered out unrealistic opportunity: {arb['profit_pct']:.1f}% profit (likely data issue)")
        
        print(f"\nScan complete!")
        print(f"Found {len(opportunities)} arbitrage opportunities")
        print(f"Sent {alerts_sent} Discord alerts")
        
        return opportunities
    
    def scan_for_arbitrage(self, matches: List[Dict], kalshi_markets: Dict, poly_markets: Dict, 
                          min_profit_pct: float = 2.0, max_profit_pct: float = 50.0, max_alerts: int = 10) -> List[Dict]:
        """Scan matched markets for arbitrage opportunities"""
        
        print(f"Scanning {len(matches)} matched markets for arbitrage opportunities...")
        print(f"Profit threshold: {min_profit_pct}% - {max_profit_pct}% (filtering out unrealistic opportunities)")
        
        opportunities = []
        alerts_sent = 0
        
        for i, match in enumerate(matches):
            if alerts_sent >= max_alerts:
                print(f"Reached maximum alert limit ({max_alerts})")
                break
                
            if i % 20 == 0:
                print(f"  Processed {i}/{len(matches)} matches...")
            
            # Get market data
            kalshi_id = match.get('kalshi_id')
            poly_id = match.get('poly_id')
            
            if not kalshi_id or not poly_id:
                continue
            
            kalshi_market = kalshi_markets.get(kalshi_id)
            poly_market = poly_markets.get(poly_id)
            
            if not kalshi_market or not poly_market:
                continue
            
            # Parse prices
            kalshi_prices = self.parse_kalshi_prices(kalshi_market)
            poly_prices = self.parse_polymarket_prices(poly_market)
            
            if not kalshi_prices or not poly_prices:
                continue
            
            # Calculate arbitrage
            arb = self.calculate_arbitrage(kalshi_prices, poly_prices, match)
            
            if arb and min_profit_pct <= arb['profit_pct'] <= max_profit_pct:
                print(f"\n🚨 ARBITRAGE FOUND:")
                print(f"   Market: {match['kalshi_title'][:60]}...")
                print(f"   Action: {arb['action']}")
                print(f"   Profit: {arb['profit_pct']:.2f}%")
                
                opportunities.append({
                    'match': match,
                    'arbitrage': arb,
                    'kalshi_market': kalshi_market,
                    'poly_market': poly_market
                })
                
                # Send Discord alert
                discord_msg = self.format_discord_message(arb, match)
                if self.send_discord_alert(discord_msg):
                    alerts_sent += 1
                    time.sleep(1)  # Rate limit protection
            
            elif arb and arb['profit_pct'] > max_profit_pct:
                print(f"   ⚠️ Filtered out unrealistic opportunity: {arb['profit_pct']:.1f}% profit (likely data issue)")
        
        print(f"\nScan complete!")
        print(f"Found {len(opportunities)} arbitrage opportunities")
        print(f"Sent {alerts_sent} Discord alerts")
        
        return opportunities
    
    def save_opportunities(self, opportunities: List[Dict], output_file: str):
        """Save arbitrage opportunities to JSON file"""
        output_data = {
            'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(),
            'total_opportunities': len(opportunities),
            'opportunities': []
        }
        
        for opp in opportunities:
            output_data['opportunities'].append({
                'match': opp['match'],
                'arbitrage': opp['arbitrage'],
                'kalshi_title': opp['match']['kalshi_title'],
                'poly_title': opp['match']['poly_title'],
                'profit_pct': opp['arbitrage']['profit_pct'],
                'action': opp['arbitrage']['action']
            })
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Saved {len(opportunities)} opportunities to {output_file}")
    
    def send_summary_alert(self, total_opportunities: int, top_profit: float):
        """Send summary alert to Discord"""
        embed = {
            "title": "📊 Arbitrage Scan Complete",
            "color": 0x0099FF,
            "fields": [
                {
                    "name": "🔍 Total Opportunities Found",
                    "value": str(total_opportunities),
                    "inline": True
                },
                {
                    "name": "🏆 Best Profit",
                    "value": f"{top_profit:.2f}%" if top_profit > 0 else "None",
                    "inline": True
                }
            ],
            "footer": {
                "text": f"PolyArbBot Scan • {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            }
        }
        
        message = {"embeds": [embed]}
        self.send_discord_alert(message)


def main():
    parser = argparse.ArgumentParser(description='Arbitrage Scanner for Prediction Markets')
    parser.add_argument('--matches', default='strict_matches.json', help='Path to matches JSON file')
    parser.add_argument('--kalshi', default='kalshi_markets.json', help='Path to Kalshi markets JSON')
    parser.add_argument('--polymarket', default='polymarket_current_active_gamma.json', help='Path to Polymarket markets JSON')
    parser.add_argument('--min-profit', type=float, default=2.0, help='Minimum profit percentage to report')
    parser.add_argument('--max-profit', type=float, default=50.0, help='Maximum profit percentage (filter out unrealistic opportunities)')
    parser.add_argument('--max-alerts', type=int, default=10, help='Maximum Discord alerts to send')
    parser.add_argument('--output', default='arbitrage_opportunities.json', help='Output file for opportunities')
    parser.add_argument('--webhook', default=DISCORD_WEBHOOK, help='Discord webhook URL')
    
    args = parser.parse_args()
    
    print("🚀 Starting Arbitrage Scanner...")
    print(f"📊 Matches file: {args.matches}")
    print(f"💰 Min profit: {args.min_profit}%")
    print(f"🔔 Max alerts: {args.max_alerts}")
    
    # Initialize scanner
    scanner = ArbScanner(webhook_url=args.webhook)
    
    # Load data
    matches = scanner.load_matches(args.matches)
    if not matches:
        print("❌ No matches loaded. Exiting.")
        return
    
    kalshi_markets, poly_markets = scanner.load_market_data(args.kalshi, args.polymarket)
    if not kalshi_markets or not poly_markets:
        print("❌ Failed to load market data. Exiting.")
        return
    
    # Scan for arbitrage
    opportunities = scanner.scan_for_arbitrage(
        matches, 
        kalshi_markets, 
        poly_markets,
        min_profit_pct=args.min_profit,
        max_profit_pct=args.max_profit,
        max_alerts=args.max_alerts
    )
    
    # Save results
    scanner.save_opportunities(opportunities, args.output)
    
    # Send summary
    top_profit = max([opp['arbitrage']['profit_pct'] for opp in opportunities], default=0)
    scanner.send_summary_alert(len(opportunities), top_profit)
    
    print("✅ Arbitrage scan complete!")


if __name__ == "__main__":
    main()
