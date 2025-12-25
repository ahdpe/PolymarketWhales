import asyncio
import logging
import os
from services.polymarket import PolymarketService
from services.telegram_service import (
    start_telegram, send_trade_alert, user_filters, 
    get_user_categories, get_default_categories, get_user_lang
)
from core.filters import get_alert_level
from core.categories import detect_category, should_show_trade
from core.localization import get_text, get_trade_level_name, get_trade_level_emoji
from config import FILTERS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Default chat ID from env (if set)
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

async def handle_trade(trade_data):
    """
    Callback for when a trade is received from Data API.
    """
    try:
        price = float(trade_data.get('price', 0))
        size = float(trade_data.get('size', 0))
        value_usd = price * size
        
        # Get alert level for this trade size
        alert_config = get_alert_level(value_usd)
        
        if not alert_config:
            return  # Trade too small for any alert
        
        # Detect category
        market_title = trade_data.get('title', 'Unknown Market')
        event_slug = trade_data.get('eventSlug', '')
        category = detect_category(market_title, event_slug)
            
        emoji = alert_config['emoji']
        side = trade_data.get('side', 'UNKNOWN')
        outcome = trade_data.get('outcome', '')
        trader = trade_data.get('name') or trade_data.get('pseudonym', 'Unknown')
        trader_address = trade_data.get('proxyWallet', '') or trade_data.get('maker', '')
        event_slug = trade_data.get('eventSlug', '')
        
        # Color for side + outcome
        # 🟢 Green = BUY Yes, 🔴 Red = BUY No, 🔵 Blue = SELL
        if side == "SELL":
            side_emoji = "🔵"
        elif outcome.lower() == "yes":
            side_emoji = "🟢"
        else:
            side_emoji = "🔴"
        
        # Category emoji
        cat_emoji = {"crypto": "💰", "sports": "⚽", "other": "📌"}.get(category, "")
        
        # Build URLs
        market_url = f"https://polymarket.com/event/{event_slug}" if event_slug else ""
        trader_url = f"https://polymarket.com/profile/{trader_address}" if trader_address else ""
        
        # Price as percentage (Polymarket prices are 0-1)
        price_pct = price * 100
        
        # Get all users who should receive this alert
        for chat_id, min_threshold in user_filters.items():
            if value_usd >= min_threshold:
                # Check active status
                from services.telegram_service import is_user_active
                if not is_user_active(chat_id):
                    continue

                # Check category filter
                user_prefs = get_user_categories(chat_id)
                if not should_show_trade(category, user_prefs):
                    continue
                
                # Get user's language
                lang = get_user_lang(chat_id)
                level_name = get_trade_level_name(lang, alert_config['min'])
                
                # Get localized emoji
                level_emoji = get_trade_level_emoji(lang, alert_config['min'])
                
                # Build trader link
                trader_text = f"[{trader}]({trader_url})" if trader_url else trader
                    
                msg = (
                    f"{cat_emoji} [{market_title[:80]}]({market_url})\n"
                    f"{side_emoji} *{side} {outcome}* @ {price_pct:.1f}%\n"
                    f"💵 *${value_usd:,.0f}*\n"
                    f"{level_emoji} {trader_text}"
                )
                await send_trade_alert(chat_id, msg)
        
        # Also send to default chat if set and not already in user_filters
        if DEFAULT_CHAT_ID:
            try:
                default_id = int(DEFAULT_CHAT_ID)
                # Skip if already sent via user_filters
                if default_id in user_filters:
                    return
                    
                # Use user's saved threshold if exists, otherwise default to lowest
                min_threshold = user_filters.get(default_id, FILTERS[-1]['min'])
                if value_usd >= min_threshold:
                    # Check category filter for default user
                    user_prefs = get_user_categories(default_id)
                    if should_show_trade(category, user_prefs):
                        lang = get_user_lang(default_id)
                        level_name = get_trade_level_name(lang, alert_config['min'])
                        level_emoji = get_trade_level_emoji(lang, alert_config['min'])
                        
                        trader_text = f"[{trader}]({trader_url})" if trader_url else trader
                        
                        msg = (
                            f"{cat_emoji} [{market_title[:80]}]({market_url})\n"
                            f"{side_emoji} *{side} {outcome}* @ {price_pct:.1f}%\n"
                            f"💵 *${value_usd:,.0f}*\n"
                            f"{level_emoji} {trader_text}"
                        )
                        await send_trade_alert(DEFAULT_CHAT_ID, msg)
            except ValueError:
                pass
                    
    except Exception as e:
        logger.error(f"Error handling trade: {e}")

async def main():
    # Start Telegram in background
    tg_task = asyncio.create_task(start_telegram())
    
    # Start Polymarket Service
    poly_service = PolymarketService()
    
    logger.info("Starting PolyWhales...")
    logger.info("Using Polymarket Data API for whale trades...")
    
    # Run Polymarket trade polling (5s interval)
    await poly_service.poll_trades(handle_trade, interval=5)
    
    await tg_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
