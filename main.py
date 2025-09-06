#!/usr/bin/env python3
"""
PolyArbBot Main Scheduler
Coordinates market data fetching, matching, and arbitrage detection

Schedule:
- Market data updates: Every 12 hours (2x daily)
- Market matching: After each data update
- Arbitrage scanning: Every 10 minutes

Usage: python main.py [--dry-run] [--once]
"""

import schedule
import time
import subprocess
import datetime as dt
import json
import logging
import sys
import argparse
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('polyarbbot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Discord webhook for status updates - set via environment variable
DISCORD_WEBHOOK = "YOUR_DISCORD_WEBHOOK_URL_HERE"

class PolyArbBot:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.base_dir = Path(__file__).parent
        self.last_market_update = None
        self.last_arb_scan = None
        
        # File paths
        self.kalshi_markets_file = self.base_dir / "kalshi_markets.json"
        self.poly_markets_file = self.base_dir / "polymarket_current_active_gamma.json"
        self.matches_file = self.base_dir / "strict_matches.json"
        self.live_prices_file = self.base_dir / "live_prices.json"
        self.arb_file = self.base_dir / "arbitrage_opportunities.json"
        
        logger.info("🚀 PolyArbBot initialized")
        if dry_run:
            logger.info("🔍 Running in DRY RUN mode - no actual execution")
    
    def run_command(self, cmd: list, description: str) -> bool:
        """Execute a command and return success status"""
        try:
            logger.info(f"▶️ {description}")
            
            if self.dry_run:
                logger.info(f"   [DRY RUN] Would execute: {' '.join(cmd)}")
                return True
            
            result = subprocess.run(
                cmd,
                cwd=self.base_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                logger.info(f"✅ {description} completed successfully")
                return True
            else:
                logger.error(f"❌ {description} failed:")
                logger.error(f"   STDOUT: {result.stdout}")
                logger.error(f"   STDERR: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ {description} timed out after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"💥 {description} crashed: {e}")
            return False
    
    def send_status_update(self, message: str, is_error: bool = False):
        """Send status update to Discord"""
        try:
            import requests
            
            color = 0xFF0000 if is_error else 0x00FF00
            embed = {
                "title": "🤖 PolyArbBot Status",
                "description": message,
                "color": color,
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "footer": {"text": "PolyArbBot Scheduler"}
            }
            
            payload = {"embeds": [embed]}
            
            if not self.dry_run:
                response = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
                if response.status_code == 204:
                    logger.info("📢 Status update sent to Discord")
                else:
                    logger.warning(f"⚠️ Discord update failed: {response.status_code}")
            else:
                logger.info(f"📢 [DRY RUN] Would send Discord update: {message}")
                
        except Exception as e:
            logger.error(f"💥 Failed to send Discord update: {e}")
    
    def fetch_market_data(self) -> bool:
        """Fetch fresh market data from both platforms"""
        logger.info("🔄 Starting market data update...")
        
        # Fetch Kalshi markets
        kalshi_success = self.run_command(
            ["python", "kalshi_fetcher.py", "--json-file", str(self.kalshi_markets_file)],
            "Fetching Kalshi markets"
        )
        
        # Fetch Polymarket markets  
        poly_success = self.run_command(
            ["python", "polymarket_fetcher.py"],
            "Fetching Polymarket markets"
        )
        
        success = kalshi_success and poly_success
        
        if success:
            self.last_market_update = dt.datetime.now()
            logger.info("✅ Market data update completed")
            self.send_status_update("📊 Market data updated successfully")
        else:
            logger.error("❌ Market data update failed")
            self.send_status_update("❌ Market data update failed", is_error=True)
        
        return success
    
    def update_market_matches(self) -> bool:
        """Update market matches using the strict matcher"""
        logger.info("🔍 Updating market matches...")
        
        success = self.run_command(
            ["python", "strict_matcher.py"],
            "Updating market matches"
        )
        
        if success:
            logger.info("✅ Market matches updated")
            
            # Log match count
            try:
                with open(self.matches_file) as f:
                    data = json.load(f)
                match_count = data.get('total_matches', 0)
                logger.info(f"📈 Found {match_count} quality market matches")
                self.send_status_update(f"🎯 Updated market matches: {match_count} quality pairs found")
            except Exception as e:
                logger.warning(f"⚠️ Could not read match count: {e}")
        else:
            logger.error("❌ Market matching failed")
            self.send_status_update("❌ Market matching failed", is_error=True)
        
        return success
    
    def fetch_live_prices(self) -> bool:
        """Fetch live prices for matched markets"""
        logger.info("💰 Fetching live prices...")
        
        success = self.run_command(
            ["python", "price_fetcher.py",
             "--matches", str(self.matches_file),
             "--output", str(self.live_prices_file)],
            "Fetching live prices"
        )
        
        if success:
            logger.info("✅ Live prices fetched")
        else:
            logger.error("❌ Live price fetch failed")
            self.send_status_update("❌ Live price fetch failed", is_error=True)
        
        return success
    
    def scan_for_arbitrage(self) -> bool:
        """Scan for arbitrage opportunities using live prices"""
        logger.info("🔍 Scanning for arbitrage opportunities...")
        
        # First fetch live prices
        if not self.fetch_live_prices():
            return False
        
        success = self.run_command(
            ["python", "-c", f"""
import sys
sys.path.append('{self.base_dir}')
from arb import ArbScanner
import json

scanner = ArbScanner()
matches = scanner.load_matches('{self.matches_file}')
live_prices = scanner.load_live_prices('{self.live_prices_file}')

if matches and live_prices:
    opportunities = scanner.scan_for_arbitrage_with_live_prices(
        matches, live_prices, 
        min_profit_pct=0.5, max_profit_pct=15.0, max_alerts=3
    )
    scanner.save_opportunities(opportunities, '{self.arb_file}')
    print(f'Found {{len(opportunities)}} opportunities')
else:
    print('Failed to load matches or live prices')
"""],
            "Scanning for arbitrage with live prices"
        )
        
        if success:
            self.last_arb_scan = dt.datetime.now()
            logger.info("✅ Arbitrage scan completed")
            
            # Log opportunity count
            try:
                with open(self.arb_file) as f:
                    data = json.load(f)
                opp_count = data.get('total_opportunities', 0)
                logger.info(f"💎 Found {opp_count} arbitrage opportunities")
                
                if opp_count == 0:
                    logger.info("🔍 No arbitrage opportunities found this scan")
                    
            except Exception as e:
                logger.warning(f"⚠️ Could not read opportunity count: {e}")
        else:
            logger.error("❌ Arbitrage scan failed")
            self.send_status_update("❌ Arbitrage scan failed", is_error=True)
        
        return success
    
    def full_update_cycle(self):
        """Complete update cycle: fetch data -> match markets -> scan arbitrage"""
        logger.info("🔄 Starting full update cycle...")
        
        start_time = dt.datetime.now()
        
        # Step 1: Fetch market data
        if not self.fetch_market_data():
            return
        
        # Step 2: Update matches
        if not self.update_market_matches():
            return
        
        # Step 3: Scan for arbitrage
        self.scan_for_arbitrage()
        
        duration = (dt.datetime.now() - start_time).total_seconds()
        logger.info(f"✅ Full update cycle completed in {duration:.1f} seconds")
    
    def quick_arb_scan(self):
        """Quick arbitrage scan with fresh prices (every 10 minutes)"""
        logger.info("⚡ Quick arbitrage scan with live prices...")
        
        # Check if we have market matches
        if not self.matches_file.exists():
            logger.warning("⚠️ No market matches found - need full update first")
            return
        
        # Always fetch fresh prices and scan for arbitrage
        self.scan_for_arbitrage()
    
    def status_report(self):
        """Generate status report"""
        logger.info("📊 Generating status report...")
        
        try:
            # Market data status
            kalshi_exists = self.kalshi_markets_file.exists()
            poly_exists = self.poly_markets_file.exists()
            matches_exist = self.matches_file.exists()
            
            # File ages
            kalshi_age = "N/A"
            poly_age = "N/A" 
            matches_age = "N/A"
            
            if kalshi_exists:
                kalshi_age = time.time() - self.kalshi_markets_file.stat().st_mtime
                kalshi_age = f"{kalshi_age/3600:.1f}h ago"
            
            if poly_exists:
                poly_age = time.time() - self.poly_markets_file.stat().st_mtime
                poly_age = f"{poly_age/3600:.1f}h ago"
                
            if matches_exist:
                matches_age = time.time() - self.matches_file.stat().st_mtime
                matches_age = f"{matches_age/3600:.1f}h ago"
            
            # Match and opportunity counts
            match_count = 0
            opp_count = 0
            
            if matches_exist:
                try:
                    with open(self.matches_file) as f:
                        data = json.load(f)
                    match_count = data.get('total_matches', 0)
                except:
                    pass
            
            if self.arb_file.exists():
                try:
                    with open(self.arb_file) as f:
                        data = json.load(f)
                    opp_count = data.get('total_opportunities', 0)
                except:
                    pass
            
            status_msg = f"""📊 **PolyArbBot Status Report**

**Market Data:**
• Kalshi: {'✅' if kalshi_exists else '❌'} ({kalshi_age})
• Polymarket: {'✅' if poly_exists else '❌'} ({poly_age})
• Matches: {'✅' if matches_exist else '❌'} ({matches_age})

**Current Stats:**
• Market pairs: {match_count}
• Active opportunities: {opp_count}

**Last Updates:**
• Market data: {self.last_market_update.strftime('%H:%M UTC') if self.last_market_update else 'Never'}
• Arbitrage scan: {self.last_arb_scan.strftime('%H:%M UTC') if self.last_arb_scan else 'Never'}"""
            
            self.send_status_update(status_msg)
            
        except Exception as e:
            logger.error(f"💥 Status report failed: {e}")
    
    def run_once(self):
        """Run one complete cycle and exit"""
        logger.info("🔄 Running single update cycle...")
        self.full_update_cycle()
        logger.info("✅ Single cycle complete - exiting")
    
    def run_scheduler(self):
        """Run the continuous scheduler"""
        logger.info("⏰ Starting PolyArbBot scheduler...")
        
        # Schedule market data updates (every 12 hours)
        schedule.every(12).hours.do(self.full_update_cycle)
        
        # Schedule arbitrage scans (every 10 minutes)
        schedule.every(10).minutes.do(self.quick_arb_scan)
        
        # Schedule status reports (every 6 hours)
        schedule.every(6).hours.do(self.status_report)
        
        # Send startup notification
        self.send_status_update("🚀 PolyArbBot scheduler started")
        
        # Run initial full update
        logger.info("🔄 Running initial update cycle...")
        self.full_update_cycle()
        
        # Main scheduler loop
        logger.info("⏰ Entering scheduler loop...")
        logger.info("📅 Schedule:")
        logger.info("   • Market data updates: Every 12 hours")
        logger.info("   • Live price + arbitrage scans: Every 10 minutes")
        logger.info("   • Status reports: Every 6 hours")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("🛑 Scheduler stopped by user")
            self.send_status_update("🛑 PolyArbBot scheduler stopped")
        except Exception as e:
            logger.error(f"💥 Scheduler crashed: {e}")
            self.send_status_update(f"💥 PolyArbBot scheduler crashed: {e}", is_error=True)
            raise


def main():
    parser = argparse.ArgumentParser(description='PolyArbBot Main Scheduler')
    parser.add_argument('--dry-run', action='store_true', help='Dry run mode - no actual execution')
    parser.add_argument('--once', action='store_true', help='Run once and exit (no continuous scheduling)')
    parser.add_argument('--status', action='store_true', help='Send status report and exit')
    
    args = parser.parse_args()
    
    bot = PolyArbBot(dry_run=args.dry_run)
    
    if args.status:
        bot.status_report()
    elif args.once:
        bot.run_once()
    else:
        bot.run_scheduler()


if __name__ == "__main__":
    main()
