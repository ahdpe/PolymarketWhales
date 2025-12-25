import asyncio
import logging
import aiohttp

logger = logging.getLogger(__name__)

# Data API endpoint (public, no auth required)
DATA_API_URL = "https://data-api.polymarket.com"

class PolymarketService:
    def __init__(self):
        self.seen_trades = set()  # To avoid duplicate notifications
        
        logger.info("PolymarketService initialized - using Data API")
        
    async def _fetch_recent_trades(self, limit=1000, min_size=500):
        """Fetch recent trades from Data API (no auth required).
        
        Args:
            limit: Maximum number of trades to fetch
            min_size: Minimum trade size in tokens (filters on API side)
        """
        try:
            # Use size_gte to filter trades on API side - this ensures we never miss
            # large trades that could be pushed out of the limit by smaller trades
            url = f"{DATA_API_URL}/trades?limit={limit}&size_gte={min_size}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        trades = await resp.json()
                        if trades and isinstance(trades, list):
                            logger.debug(f"Fetched {len(trades)} trades from Data API")
                            return trades
                        return []
                    else:
                        text = await resp.text()
                        logger.error(f"Failed to fetch trades: {resp.status} - {text[:200]}")
                        return []
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return []

    async def poll_trades(self, callback, interval=5):
        """
        Poll for new trades every `interval` seconds.
        Call `callback(trade_dict)` for each new trade.
        """
        logger.info(f"Starting trade polling (every {interval}s)...")
        
        while True:
            try:
                trades = await self._fetch_recent_trades(limit=1000)
                
                if trades:
                    new_count = 0
                    for trade in trades:
                        # Generate unique trade ID from transaction hash
                        trade_id = trade.get('transactionHash', '') or str(hash(str(trade)))
                        
                        if trade_id not in self.seen_trades:
                            self.seen_trades.add(trade_id)
                            new_count += 1
                            
                            # Keep seen_trades from growing too large
                            if len(self.seen_trades) > 10000:
                                self.seen_trades = set(list(self.seen_trades)[-5000:])
                            
                            await callback(trade)
                    
                    if new_count > 0:
                        logger.info(f"Processed {new_count} new trades")
                    
            except Exception as e:
                logger.error(f"Polling error: {e}")
            
            await asyncio.sleep(interval)
