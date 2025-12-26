
import asyncio

# Mock user filters
user_filters = {
    176487802: 50000,
    162658799: 25000,
}

# Mock categories
def get_user_categories(chat_id):
    return {'all': True, 'other': True, 'crypto': True, 'sports': True}

def should_show_trade(category, prefs):
    return True

def get_alert_level(value):
    return {'min': 25000, 'emoji': '🐋', 'name': 'Whale'} if value >= 25000 else None

async def send_trade_alert(chat_id, msg):
    print(f"ALERT SENT TO {chat_id}: {msg}")

async def handle_trade_mock(trade_data):
    price = float(trade_data.get('price', 0))
    size = float(trade_data.get('size', 0))
    value_usd = price * size
    
    print(f"Processing trade: Value=${value_usd:,.2f} Size=${size:,.2f}")

    # Loop from main.py
    for chat_id, min_threshold in user_filters.items():
        # print(f"Checking user {chat_id} with threshold {min_threshold}")
        if value_usd >= min_threshold:
            print(f" -> MATCH! User {chat_id} threshold {min_threshold} passed.")
            msg = "Trade Alert"
            await send_trade_alert(chat_id, msg)
        else:
            print(f" -> REJECT. User {chat_id} threshold {min_threshold} > {value_usd}")

# Simulate the trade
trade = {
    'price': 0.999,
    'size': 39900, # shares/payout
    'title': 'Will Elon Musk post...',
    'side': 'BUY',
    'outcome': 'No'
}

asyncio.run(handle_trade_mock(trade))
