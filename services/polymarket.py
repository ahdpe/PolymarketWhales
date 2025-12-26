import asyncio
import logging
import aiohttp
import time
import sqlite3
import os
from decimal import Decimal
from collections import OrderedDict

logger = logging.getLogger(__name__)

# Data API endpoint (public, no auth required)
DATA_API_URL = "https://data-api.polymarket.com"

# Polling configuration
POLL_INTERVAL = 3
MAX_LRU_SIZE = 10000
DB_PATH = "data/trades.db"
TTL_HOURS = 72


class TradePersistence:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.lru = OrderedDict()
        self._init_db()
        self.last_cleanup = time.time()

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        # Performance tuning
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA temp_store=MEMORY;")
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_trades (
                trade_key TEXT PRIMARY KEY,
                seen_at INTEGER NOT NULL
            );
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_seen_at ON seen_trades(seen_at);")
        self.conn.commit()

    def _normalize_decimal(self, val):
        try:
            return str(Decimal(str(val)).quantize(Decimal("0.000001")))
        except:
            return "0.000000"

    def generate_key(self, trade):
        # Normalization
        price = self._normalize_decimal(trade.get('price', 0))
        size = self._normalize_decimal(trade.get('size', 0))
        try:
            ts = int(trade.get('timestamp', 0))
        except:
            ts = 0
        
        parts = [
            trade.get('proxyWallet', ''),
            trade.get('conditionId', ''),
            trade.get('side', ''),
            trade.get('outcomeIndex', ''),
            price,
            size,
            ts,
            trade.get('transactionHash', '')
        ]
        return "|".join(str(p) for p in parts)

    def is_seen(self, key):
        # 1. Check LRU
        if key in self.lru:
            self.lru.move_to_end(key)
            return True
        
        # 2. Check DB
        cursor = self.conn.execute("SELECT 1 FROM seen_trades WHERE trade_key=? LIMIT 1", (key,))
        if cursor.fetchone():
            self._add_to_lru(key)
            return True
        
        return False

    def _add_to_lru(self, key):
        self.lru[key] = None
        self.lru.move_to_end(key)
        if len(self.lru) > MAX_LRU_SIZE:
            self.lru.popitem(last=False)

    def add_batch(self, keys):
        if not keys:
            return
        
        now_ms = int(time.time() * 1000)
        data = [(k, now_ms) for k in keys]
        
        with self.conn:
            self.conn.executemany(
                "INSERT OR IGNORE INTO seen_trades(trade_key, seen_at) VALUES (?, ?)",
                data
            )
        
        for k in keys:
            self._add_to_lru(k)

    def cleanup(self):
        # Run cleanup once hour
        if time.time() - self.last_cleanup < 3600:
            return

        logger.info("Running DB cleanup...")
        cutoff_ms = int((time.time() - (TTL_HOURS * 3600)) * 1000)
        with self.conn:
            self.conn.execute("DELETE FROM seen_trades WHERE seen_at < ?", (cutoff_ms,))
        self.last_cleanup = time.time()
        logger.info("DB cleanup completed")

    def close(self):
        self.conn.close()



class TradeAggregator:
    def __init__(self, window_sec=60, min_alert_usd=500):
        self.window_sec = window_sec
        self.min_alert_usd = min_alert_usd
        self.series = {}  # key -> SeriesData
        self.last_cleanup = time.time()

    def _get_key(self, trade):
        return (
            trade.get('proxyWallet', ''),
            trade.get('conditionId', ''),
            trade.get('side', ''),
            trade.get('outcomeIndex', str(trade.get('outcome', '')))
        )

    def process_trade(self, trade):
        """
        Process a new trade.
        Returns: aggregated_trade dict if a series triggers an alert, else None.
        """
        key = self._get_key(trade)
        now_ts = trade.get('timestamp', time.time())
        try:
             # Ensure timestamp is int/float
            now_ts = float(now_ts)
        except:
            now_ts = time.time()

        price = float(trade.get('price', 0))
        size = float(trade.get('size', 0))
        usd_val = price * size

        # Check if series exists and is within window
        if key in self.series:
            s = self.series[key]
            # Window check: 60s from first trade
            if now_ts - s['first_ts'] > self.window_sec:
                # Series expired, close old one and start new
                del self.series[key]
                s = None
        else:
            s = None

        if s is None:
            # Start new series
            s = {
                'first_ts': now_ts,
                'last_ts': now_ts,
                'usd_sum': 0.0,
                'size_sum': 0.0,
                'volume_weighted_price_sum': 0.0, # price * size sum for VWAP
                'fills': 0,
                'alert_sent': False,
                'base_trade': trade # Keep reference for metadata (title, slug, etc)
            }
            self.series[key] = s

        # Update series
        s['last_ts'] = max(s['last_ts'], now_ts)
        s['usd_sum'] += usd_val
        s['size_sum'] += size
        s['volume_weighted_price_sum'] += (price * size)
        s['fills'] += 1

        # Check Trigger
        if s['usd_sum'] >= self.min_alert_usd and not s['alert_sent']:
            s['alert_sent'] = True
            
            # Construct Aggregate Trade Object
            avg_price = s['volume_weighted_price_sum'] / s['size_sum'] if s['size_sum'] > 0 else 0
            
            agg_trade = s['base_trade'].copy()
            agg_trade.update({
                'is_aggregate': True,
                'series_fills': s['fills'],
                'series_usd_sum': s['usd_sum'],
                'series_avg_price': avg_price,
                'series_window_sec': self.window_sec,
                # Override size/price with totals for accurate display logic
                'size': s['size_sum'], 
                'price': avg_price,
                'value_usd': s['usd_sum'] # Pre-calculate for main.py
            })
            return agg_trade
        
        return None

    def cleanup(self):
        """Garbage collect old series."""
        if time.time() - self.last_cleanup < 10:
            return
            
        now = time.time()
        keys_to_del = []
        for k, s in self.series.items():
            # If nothing happened for window_sec + buffer, delete
            if now - s['last_ts'] > self.window_sec + 10:
                keys_to_del.append(k)
        
        for k in keys_to_del:
            del self.series[k]
            
        self.last_cleanup = now


class PolymarketService:
    def __init__(self):
        self.persistence = TradePersistence()
        self.aggregator = TradeAggregator(window_sec=60, min_alert_usd=500)
        self.last_timestamp = 0
        self.consecutive_errors = 0
        self.total_trades_processed = 0
        
        logger.info("PolymarketService initialized - using Data API with SQLite Persistence & Aggregation")
        
    async def _fetch_recent_trades(self, limit=10000, offset=0, min_size=10):
        """Fetch recent trades from Data API."""
        try:
            # Optimized API request with server-side filtering
            # Lowered min_size to 10 to capture shards for aggregation
            url = f"{DATA_API_URL}/trades?limit={limit}&offset={offset}&takerOnly=true&filterType=CASH&filterAmount={min_size}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        trades = await resp.json()
                        if trades and isinstance(trades, list):
                            self.consecutive_errors = 0
                            return trades
                        return []
                    else:
                        text = await resp.text()
                        self.consecutive_errors += 1
                        logger.error(f"Failed to fetch trades: {resp.status} - {text[:200]}")
                        return []
        except asyncio.TimeoutError:
            self.consecutive_errors += 1
            logger.error("Timeout fetching trades from Data API")
            return []
        except Exception as e:
            self.consecutive_errors += 1
            logger.error(f"Error fetching trades: {e}")
            return []

    async def poll_trades(self, callback, interval=POLL_INTERVAL):
        """
        Poll for new trades every `interval` seconds.
        Uses pagination, SQLite persistence, and Aggregation.
        """
        logger.info(f"Starting trade polling (every {interval}s)...")
        limit = 10000 
        
        while True:
            try:
                offset = 0
                max_pages = 5
                trades_found_in_poll = 0
                new_keys_batch = []
                
                for page in range(max_pages):
                    trades = await self._fetch_recent_trades(limit=limit, offset=offset)
                    
                    if not trades:
                        break
                        
                    # Sort by timestamp (oldest first)
                    trades_sorted = sorted(trades, key=lambda t: t.get('timestamp', 0))
                    
                    oldest_trade_ts = trades_sorted[0].get('timestamp', 0)
                    newest_trade_ts = trades_sorted[-1].get('timestamp', 0)
                    
                    for trade in trades_sorted:
                        key = self.persistence.generate_key(trade)
                        
                        if self.persistence.is_seen(key):
                            continue
                        
                        # New trade confirmed
                        self.persistence._add_to_lru(key)
                        new_keys_batch.append(key)
                        trades_found_in_poll += 1
                        self.total_trades_processed += 1
                        
                        # Pass to Aggregator
                        agg_trade = self.aggregator.process_trade(trade)
                        if agg_trade:
                            # If aggregator triggered a series alert, send IT
                            await callback(agg_trade)
                            
                        # If you wanted to support single non-aggregated alerts for random big trades, 
                        # you could add logic here. But per request, we focus on Aggregate >= 500.
                        # Note: _fetch_recent_trades filters < 10. Aggregator filters sum < 500.
                        # So a single trade of $1000 will be aggregated immediately (fills=1) and sent.

                    # Update global last timestamp
                    if newest_trade_ts > self.last_timestamp:
                        self.last_timestamp = newest_trade_ts

                    # Robustness Check
                    if self.last_timestamp > 0 and oldest_trade_ts > self.last_timestamp:
                        logger.info(f"Gap detected! Oldest fetch: {oldest_trade_ts}, Last seen: {self.last_timestamp}. Paging deeper (offset {offset + limit})...")
                        offset += limit
                    else:
                        break
                
                # Batch insert new keys to DB
                if new_keys_batch:
                    self.persistence.add_batch(new_keys_batch)
                    logger.info(f"Processed {len(new_keys_batch)} new raw trades. Aggregator active.")
                
                # Aggregator Cleanup
                self.aggregator.cleanup()

                # Persistence Cleanup
                self.persistence.cleanup()
                
                # Health Check
                if self.consecutive_errors >= 3:
                     logger.warning(f"Data API experiencing issues ({self.consecutive_errors} consecutive errors)")
                     
            except Exception as e:
                logger.error(f"Polling error: {e}")
            
            await asyncio.sleep(interval)
    
    def get_stats(self):
        """Get service statistics."""
        return {
            "total_processed": self.total_trades_processed,
            "lru_size": len(self.persistence.lru),
            "active_series": len(self.aggregator.series),
            "last_timestamp": self.last_timestamp,
            "consecutive_errors": self.consecutive_errors
        }
