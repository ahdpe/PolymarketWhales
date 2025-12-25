from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton
)
import logging
from config import TELEGRAM_BOT_TOKEN, FILTERS, OWNER_ID
from core.localization import get_text, get_trade_level_name

logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# Settings file path
import os
import json
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), '..', 'user_settings.json')

def load_settings():
    """Load user settings from file."""
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                data = json.load(f)
                # Convert string keys back to int
                filters = {int(k): v for k, v in data.get('filters', {}).items()}
                categories = {int(k): v for k, v in data.get('categories', {}).items()}
                languages = {int(k): v for k, v in data.get('languages', {}).items()}
                statuses = {int(k): v for k, v in data.get('statuses', {}).items()}
                usernames = {int(k): v for k, v in data.get('usernames', {}).items()}
                return filters, categories, languages, statuses, usernames
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
    return {}, {}, {}, {}, {}

def save_settings():
    """Save user settings to file."""
    try:
        data = {
            'filters': {str(k): v for k, v in user_filters.items()},
            'categories': {str(k): v for k, v in user_categories.items()},
            'languages': {str(k): v for k, v in user_languages.items()},
            'statuses': {str(k): v for k, v in user_statuses.items()},
            'usernames': {str(k): v for k, v in user_usernames.items()}
        }
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Error saving settings: {e}")

# Load settings on startup
user_filters, user_categories, user_languages, user_statuses, user_usernames = load_settings()

def get_default_categories():
    """Default category preferences - all enabled."""
    return {'all': True, 'other': True, 'crypto': True, 'sports': True}

def get_user_lang(chat_id):
    """Get user's language preference."""
    return user_languages.get(chat_id, 'ru')

def is_user_active(chat_id):
    """Check if user bot is active (started)."""
    return user_statuses.get(chat_id, True)  # Default True (Active)

def get_main_keyboard(chat_id):
    """Create persistent keyboard at bottom of chat."""
    lang = get_user_lang(chat_id)
    active = is_user_active(chat_id)
    
    # Toggle button text
    btn_toggle = get_text(lang, 'btn_stop') if active else get_text(lang, 'btn_start')
    
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=get_text(lang, 'btn_filter')),
             KeyboardButton(text=btn_toggle)],
            [KeyboardButton(text=get_text(lang, 'btn_language')),
             KeyboardButton(text=get_text(lang, 'btn_about'))]
        ],
        resize_keyboard=True,
        is_persistent=True
    )

def get_unified_keyboard(chat_id):
    """Create unified inline keyboard (amounts + categories)."""
    lang = get_user_lang(chat_id)
    prefs = user_categories.get(chat_id, get_default_categories())
    current_min = user_filters.get(chat_id, 50000)  # Default $50k
    
    buttons = []
    
    # 1. Amount Filters
    # Header
    buttons.append([InlineKeyboardButton(
        text=get_text(lang, 'filter_section_amount'),
        callback_data="ignore"
    )])
    
    for f in FILTERS:
        # Mark selected filter
        text = f"{f['emoji']} >${f['min']:,}"
        if f['min'] == current_min:
            text = f"✅ {text}"
            
        btn = InlineKeyboardButton(text=text, callback_data=f"filter_{f['min']}")
        buttons.append([btn])
        
    # 2. Category Filters
    # Header
    buttons.append([InlineKeyboardButton(
        text=get_text(lang, 'filter_section_category'),
        callback_data="ignore"
    )])
    
    def check(key):
        return "✅" if prefs.get(key, True) else "⬜"
    
    buttons.append([InlineKeyboardButton(
        text=f"{check('all')} {get_text(lang, 'settings_all')}",
        callback_data="cat_all"
    )])
    buttons.append([InlineKeyboardButton(
        text=f"{check('other')} {get_text(lang, 'settings_other')}",
        callback_data="cat_other"
    )])
    buttons.append([InlineKeyboardButton(
        text=f"{check('crypto')} {get_text(lang, 'settings_crypto')}",
        callback_data="cat_crypto"
    )])
    buttons.append([InlineKeyboardButton(
        text=f"{check('sports')} {get_text(lang, 'settings_sports')}",
        callback_data="cat_sports"
    )])
    
    # Done button
    buttons.append([InlineKeyboardButton(
        text=get_text(lang, 'settings_done'),
        callback_data="cat_done"
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    chat_id = message.chat.id
    # Set defaults
    if chat_id not in user_filters:
        user_filters[chat_id] = 50000  # Default to $50k
    if chat_id not in user_categories:
        user_categories[chat_id] = get_default_categories()
    if chat_id not in user_languages:
        user_languages[chat_id] = 'ru'
    
    # Save username/name
    username = message.from_user.username or message.from_user.first_name or str(chat_id)
    user_usernames[chat_id] = username
    
    # Force active on start command
    user_statuses[chat_id] = True
    save_settings()
    
    lang = get_user_lang(chat_id)
    await message.answer(
        get_text(lang, 'welcome', chat_id=chat_id),
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(chat_id)
    )
    logger.info(f"User started bot. Chat ID: {chat_id}")

@dp.message(Command("filter"))
async def cmd_filter(message: types.Message):
    """Show unified filter/settings menu."""
    chat_id = message.chat.id
    lang = get_user_lang(chat_id)
    if chat_id not in user_categories:
        user_categories[chat_id] = get_default_categories()
        
    await message.answer(
        get_text(lang, 'filter_menu_title'),
        parse_mode="Markdown",
        reply_markup=get_unified_keyboard(chat_id)
    )

# Text handlers for bottom keyboard buttons
@dp.message(F.text.in_(["⚙️ Фильтр и Настройки", "⚙️ Filter & Settings"]))
async def btn_filter(message: types.Message):
    """Handle Filter button press."""
    await cmd_filter(message)

@dp.message(F.text.in_(["▶️ Запустить", "▶️ Start", "⏸️ Остановить", "⏸️ Stop"]))
async def btn_start_stop(message: types.Message):
    """Handle Start/Stop toggle button."""
    chat_id = message.chat.id
    lang = get_user_lang(chat_id)
    active = is_user_active(chat_id)
    
    # Toggle state
    new_state = not active
    user_statuses[chat_id] = new_state
    save_settings()
    
    msg_key = 'bot_started' if new_state else 'bot_stopped'
    
    await message.answer(
        get_text(lang, msg_key),
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(chat_id)
    )

@dp.message(F.text.in_(["🌐 EN", "🌐 RU"]))
async def btn_language(message: types.Message):
    """Handle Language toggle button."""
    chat_id = message.chat.id
    current_lang = get_user_lang(chat_id)
    
    # Toggle language
    new_lang = 'en' if current_lang == 'ru' else 'ru'
    user_languages[chat_id] = new_lang
    save_settings()
    
    await message.answer(
        get_text(new_lang, 'welcome', chat_id=chat_id),
        parse_mode="Markdown",
        reply_markup=get_main_keyboard(chat_id)
    )

@dp.message(F.text.in_(["ℹ️ О боте", "ℹ️ About"]))
async def btn_about(message: types.Message):
    """Handle About button press."""
    chat_id = message.chat.id
    lang = get_user_lang(chat_id)
    
    await message.answer(
        get_text(lang, 'about'),
        parse_mode="Markdown"
    )

@dp.callback_query(F.data.startswith("filter_"))
async def callback_filter(callback: CallbackQuery):
    """Handle filter amount selection."""
    chat_id = callback.message.chat.id
    min_value = int(callback.data.replace("filter_", ""))
    
    user_filters[chat_id] = min_value
    save_settings()
    
    # Refresh keyboard to show checkmark on new selection
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=get_unified_keyboard(chat_id))
    logger.info(f"User {chat_id} set filter to ${min_value}")

@dp.callback_query(F.data.startswith("cat_"))
async def callback_category(callback: CallbackQuery):
    """Handle category toggle callback."""
    chat_id = callback.message.chat.id
    lang = get_user_lang(chat_id)
    category = callback.data.replace("cat_", "")
    
    if chat_id not in user_categories:
        user_categories[chat_id] = get_default_categories()
    
    prefs = user_categories[chat_id]
    
    if category == "done":
        # Close settings
        save_settings()
        
        # Get active categories text
        enabled = [k for k, v in prefs.items() if v and k != 'all']
        labels = {
            'other': get_text(lang, 'cat_other'),
            'crypto': get_text(lang, 'cat_crypto'),
            'sports': get_text(lang, 'cat_sports')
        }
        enabled_text = ", ".join(labels.get(k, k) for k in enabled) if enabled else get_text(lang, 'cat_nothing')
        
        # Get active filter text
        min_val = user_filters.get(chat_id, 50000)
        
        await callback.answer(get_text(lang, 'filter_toast'))
        await callback.message.edit_text(
            get_text(lang, 'filter_set', min=min_val, categories=enabled_text),
            parse_mode="Markdown"
        )
        return
    
    if category == "all":
        # Toggle all
        new_state = not prefs.get('all', True)
        prefs['all'] = new_state
        prefs['other'] = new_state
        prefs['crypto'] = new_state
        prefs['sports'] = new_state
    else:
        # Toggle individual category
        prefs[category] = not prefs.get(category, True)
        prefs['all'] = prefs.get('other', False) and prefs.get('crypto', False) and prefs.get('sports', False)
    
    user_categories[chat_id] = prefs
    
    # Don't save on every click to avoid disk IO, save only on 'done' or use logic to save periodically?
    # For now saving on every click to be safe against crashes, or rely on 'done'
    # Let's save on 'done' mostly, but maybe better to save here too just in case
    # Actually saving here ensures consistent state if they just close chat
    
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=get_unified_keyboard(chat_id))

def get_user_min_threshold(chat_id):
    """Get user's minimum threshold. Return default if not set."""
    return user_filters.get(chat_id, FILTERS[-1]['min'])

def get_user_categories(chat_id):
    """Get user's category preferences."""
    return user_categories.get(chat_id, get_default_categories())

def is_user_active(chat_id):
    """Check if user is active."""
    return user_statuses.get(chat_id, True)


# ============ ADMIN COMMANDS (Owner Only) ============

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Show bot statistics (owner only)."""
    if message.chat.id != OWNER_ID:
        return  # Silently ignore non-owners
    
    total_users = len(user_filters)
    active_users = sum(1 for uid in user_statuses if user_statuses.get(uid, True))
    paused_users = total_users - active_users
    
    # Filter distribution
    filter_dist = {}
    for uid, threshold in user_filters.items():
        filter_dist[threshold] = filter_dist.get(threshold, 0) + 1
    
    # Category preferences
    crypto_on = sum(1 for uid in user_categories if user_categories[uid].get('crypto', True))
    sports_on = sum(1 for uid in user_categories if user_categories[uid].get('sports', True))
    other_on = sum(1 for uid in user_categories if user_categories[uid].get('other', True))
    
    # Language distribution
    ru_users = sum(1 for uid in user_languages if user_languages.get(uid, 'ru') == 'ru')
    en_users = total_users - ru_users
    
    msg = f"""📊 **Статистика бота**

👥 **Пользователи:** {total_users}
▶️ Активных: {active_users}
⏸️ На паузе: {paused_users}

💰 **Фильтры по сумме:**
"""
    for f in FILTERS:
        count = filter_dist.get(f['min'], 0)
        msg += f"  {f['emoji']}: {count}\n"
    
    msg += f"""
📂 **Категории:**
💰 Крипто вкл: {crypto_on}
⚽ Спорт вкл: {sports_on}
📌 Остальное вкл: {other_on}

🌐 **Язык:**
🇷🇺 RU: {ru_users}
🇬🇧 EN: {en_users}
"""
    
    await message.answer(msg, parse_mode="Markdown")


@dp.message(Command("users"))
async def cmd_users(message: types.Message):
    """List all users (owner only)."""
    if message.chat.id != OWNER_ID:
        return  # Silently ignore non-owners
    
    if not user_filters:
        await message.answer("📭 Пока нет пользователей.")
        return
    
    msg = "👥 **Список пользователей:**\n\n"
    for uid in list(user_filters.keys())[:50]:  # Limit to 50
        threshold = user_filters.get(uid, 100)
        status = "▶️" if user_statuses.get(uid, True) else "⏸️"
        lang = user_languages.get(uid, 'ru').upper()
        username = user_usernames.get(uid, "—")
        msg += f"@{username} | {status} | ${threshold:,} | {lang}\n"
    
    if len(user_filters) > 50:
        msg += f"\n... и ещё {len(user_filters) - 50} пользователей"
    
    await message.answer(msg, parse_mode="Markdown")


async def start_telegram():
    logger.info("Starting Telegram Bot Polling...")
    await dp.start_polling(bot)

async def send_trade_alert(chat_id, message_text):
    if not chat_id:
        return
    try:
        await bot.send_message(chat_id=chat_id, text=message_text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")
