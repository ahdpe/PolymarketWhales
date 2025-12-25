import asyncio
import logging
import aiohttp
import time

logger = logging.getLogger(__name__)

# Data API endpoint (public, no auth required)
DATA_API_URL = "https://data-api.polymarket.com"

# Polling configuration
POLL_INTERVAL = 3  # Reduced from 5 seconds for faster updates
MAX_SEEN_TRADES = 50000  # Increased cache size
CACHE_TRIM_SIZE = 25000  # Keep this many when trimming


class PolymarketService:
    def __init__(self):
        self.seen_trades = set()  # To avoid duplicate notifications
        self.last_timestamp = 0  # Track last seen timestamp
        self.consecutive_errors = 0
        self.total_trades_processed = 0
        
        logger.info("PolymarketService initialized - using Data API")
        
    async def _fetch_recent_trades(self, limit=1000):
        """Fetch recent trades from Data API (no auth required).
        
        Args:
            limit: Maximum number of trades to fetch
        """
        try:
            # Note: size_gte parameter doesn't work reliably, so we fetch all and filter locally
            url = f"{DATA_API_URL}/trades?limit={limit}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        trades = await resp.json()
                        if trades and isinstance(trades, list):
                            self.consecutive_errors = 0
                            logger.debug(f"Fetched {len(trades)} trades from Data API")
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
        Call `callback(trade_dict)` for each new trade.
        """
        logger.info(f"Starting trade polling (every {interval}s, cache size {MAX_SEEN_TRADES})...")
        
        while True:
            try:
                trades = await self._fetch_recent_trades(limit=1000)
                
                if trades:
                    new_count = 0
                    
                    # Sort by timestamp to process in order (oldest first)
                    trades_sorted = sorted(trades, key=lambda t: t.get('timestamp', 0))
                    
                    for trade in trades_sorted:
                        # Generate unique trade ID from transaction hash
                        trade_id = trade.get('transactionHash', '') or str(hash(str(trade)))
                        
                        if trade_id not in self.seen_trades:
                            self.seen_trades.add(trade_id)
                            new_count += 1
                            self.total_trades_processed += 1
                            
                            # Update last timestamp
                            trade_ts = trade.get('timestamp', 0)
                            if trade_ts > self.last_timestamp:
                                self.last_timestamp = trade_ts
                            
                            # Keep seen_trades from growing too large
                            if len(self.seen_trades) > MAX_SEEN_TRADES:
                                # Convert to list, keep last CACHE_TRIM_SIZE items
                                self.seen_trades = set(list(self.seen_trades)[-CACHE_TRIM_SIZE:])
                                logger.info(f"Trimmed seen_trades cache to {len(self.seen_trades)}")
                            
                            await callback(trade)
                    
                    if new_count > 0:
                        logger.info(f"Processed {new_count} new trades (total: {self.total_trades_processed})")
                
                # Log health status periodically
                if self.consecutive_errors >= 3:
                    logger.warning(f"Data API experiencing issues ({self.consecutive_errors} consecutive errors)")
                    
            except Exception as e:
                logger.error(f"Polling error: {e}")
            
            await asyncio.sleep(interval)
    
    def get_stats(self):
        """Get service statistics."""
        return {
            "total_processed": self.total_trades_processed,
            "cache_size": len(self.seen_trades),
            "last_timestamp": self.last_timestamp,
            "consecutive_errors": self.consecutive_errors
        }
