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


class PolymarketService:
    def __init__(self):
        self.persistence = TradePersistence()
        self.last_timestamp = 0
        self.consecutive_errors = 0
        self.total_trades_processed = 0
        
        logger.info("PolymarketService initialized - using Data API with SQLite Persistence")
        
    async def _fetch_recent_trades(self, limit=10000, offset=0, min_size=500):
        """Fetch recent trades from Data API."""
        try:
            # Optimized API request with server-side filtering
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
        Uses pagination and SQLite persistence.
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
                        
                        # New trade
                        self.persistence._add_to_lru(key) # Speculative add to avoid dupes in same batch if any
                        new_keys_batch.append(key)
                        
                        await callback(trade)
                        
                        trades_found_in_poll += 1
                        self.total_trades_processed += 1

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
                    logger.info(f"Processed {len(new_keys_batch)} new trades (total: {self.total_trades_processed})")
                
                # Cleanup DB
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
            "last_timestamp": self.last_timestamp,
            "consecutive_errors": self.consecutive_errors
        }
