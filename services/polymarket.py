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
        
    async def _fetch_recent_trades(self, limit=10000, offset=0, min_size=500):
        """Fetch recent trades from Data API (no auth required).
        
        Args:
            limit: Maximum number of trades to fetch
            min_size: Minimum trade size in USD to filter by
        """
        try:
            # Optimized API request with server-side filtering
            # limit: Max records (increased to 10000 to broaden window)
            # filterType=CASH & filterAmount: Only fetch trades > $val
            # takerOnly=true: Focus on market moves
            url = f"{DATA_API_URL}/trades?limit={limit}&offset={offset}&takerOnly=true&filterType=CASH&filterAmount={min_size}"
            
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
        Uses pagination to ensure no gaps in trade history.
        """
        logger.info(f"Starting trade polling (every {interval}s, cache size {MAX_SEEN_TRADES})...")
        limit = 10000 
        
        while True:
            try:
                offset = 0
                max_pages = 5 # Safety limit to prevent infinite loops
                trades_found_in_poll = 0
                
                for page in range(max_pages):
                    trades = await self._fetch_recent_trades(limit=limit, offset=offset)
                    
                    if not trades:
                        break
                        
                    # Sort by timestamp to process in order (oldest first)
                    # Note: API usually returns newest first, so we reverse for processing
                    trades_sorted = sorted(trades, key=lambda t: t.get('timestamp', 0))
                    
                    # Check coverage
                    oldest_trade_ts = trades_sorted[0].get('timestamp', 0)
                    newest_trade_ts = trades_sorted[-1].get('timestamp', 0)
                    
                    # Deduplicate and callback
                    new_count = 0
                    for trade in trades_sorted:
                        trade_id = trade.get('transactionHash', '') or str(hash(str(trade)))
                        
                        if trade_id not in self.seen_trades:
                            self.seen_trades.add(trade_id)
                            new_count += 1
                            self.total_trades_processed += 1
                            trades_found_in_poll += 1
                            
                            # Keep seen_trades from growing too large
                            if len(self.seen_trades) > MAX_SEEN_TRADES:
                                self.seen_trades = set(list(self.seen_trades)[-CACHE_TRIM_SIZE:])
                            
                            await callback(trade)

                    # Update global last timestamp from newest trade in batch
                    if newest_trade_ts > self.last_timestamp:
                        self.last_timestamp = newest_trade_ts

                    # Robustness Check: Do we need to go deeper?
                    # If the oldest trade in this batch is still newer than what we've seen before,
                    # we might have missed trades in the gap.
                    # Only relevant if we have a history (last_timestamp > 0)
                    if self.last_timestamp > 0 and oldest_trade_ts > self.last_timestamp:
                        logger.info(f"Gap detected! Oldest fetch: {oldest_trade_ts}, Last seen: {self.last_timestamp}. Paging deeper (offset {offset + limit})...")
                        offset += limit
                    else:
                        # We have overlapped with known history, or this is first run.
                        # Stop paging.
                        break
                
                if trades_found_in_poll > 0:
                     logger.info(f"Processed {trades_found_in_poll} new trades (total: {self.total_trades_processed})")

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
