"""Localization strings for PolyWhales bot."""

TRANSLATIONS = {
    'ru': {
        # Start message
        'welcome': "Привет! Я бот PolyWhales. 🐋\nТвой ID чата: `{chat_id}`\n\nИспользуй кнопки внизу для настройки.",
        
        # Buttons
        'btn_filter': "💰 Фильтр",
        # Buttons
        'btn_filter': "⚙️ Фильтр и Настройки",
        'btn_start': "▶️ Запустить",
        'btn_stop': "⏸️ Остановить",
        'btn_language': "🌐 EN",
        'btn_about': "ℹ️ О боте",
        
        # Status
        'bot_started': "▶️ **Бот запущен!**\nЯ буду присылать уведомления о сделках.",
        'bot_stopped': "⏸️ **Бот остановлен.**\nУведомления приходить не будут, пока ты снова не запустишь бота.",
        
        # Filter
        # Filter
        'filter_menu_title': "⚙️ **Фильтры и Настройки**\n\nВыбери минимальную сумму сделки и категории:",
        'filter_section_amount': "➖➖➖ СУММА СДЕЛКИ ➖➖➖",
        'filter_section_category': "➖➖➖ КАТЕГОРИИ ➖➖➖",
        'filter_set': "✅ Настройки обновлены!\n\n**Порог:** ${min:,}\n**Категории:** {categories}",
        'filter_toast': "Настройки обновлены!",
        
        # Settings
        'settings_title': "⚙️ **Настройки категорий**\n\nВыбери какие рынки отслеживать:",
        'settings_all': "Все сделки",
        'settings_other': "Всё кроме крипты и спорта",
        'settings_crypto': "💰 Крипто",
        'settings_sports': "⚽ Спорт",
        'settings_done': "✔️ Готово",
        'settings_saved': "✅ **Настройки сохранены!**\n\nАктивные категории: {categories}",
        'settings_toast': "Настройки сохранены!",
        'cat_other': "Остальное",
        'cat_crypto': "Крипто",
        'cat_sports': "Спорт",
        'cat_nothing': "Ничего",
        
        # About
        'about': """*Polymarket Whales* 🐋
Мониторинг крупных сделок на [Polymarket](https://polymarket.com) в реальном времени.

*Функционал:*
• Уведомления о сделках от $500 до $100,000+
• Фильтр минимальной суммы (настраивается пользователем)
• Выбор категорий (Крипто, Спорт, Остальное)

*Классификация объемов:*
🔥 МЕГА КИТ — >$100,000
⚡ СУПЕР КИТ — >$50,000
🐋 КИТ — >$25,000
🦈 АКУЛА — >$10,000
🐬 ДЕЛЬФИН — >$5,000
🐟 РЫБА — >$2,000
🦐 КРЕВЕТКА — >$500

*Как определяются категории:*
1. 💰 *Крипто (Crypto)*
Если в названии есть: bitcoin, btc, ethereum, eth, solana, doge, pepe, binance, nft, airdrop и др.
2. ⚽ *Спорт (Sports)*
Если в названии есть: nfl, nba, football, soccer, ufc, f1, lakers, goal и др.

💬 Обратная связь: @Andrey\_Os

⚡ *ТОП Биржа для торговли:*
[Регистрируйся на Bybit и получи бонусы! 🎁](https://www.bybit.com/invite?ref=JDRKDN)""",
        
        # Trade alerts
        'open_market': "Открыть рынок",
    },
    
    'en': {
        # Start message
        'welcome': "Hello! I'm PolyWhales bot. 🐋\nYour chat ID: `{chat_id}`\n\nUse the buttons below to configure.",
        
        # Buttons
        'btn_filter': "⚙️ Filter & Settings",
        'btn_start': "▶️ Start",
        'btn_stop': "⏸️ Stop",
        'btn_language': "🌐 RU",
        'btn_about': "ℹ️ About",
        
        # Status
        'bot_started': "▶️ **Bot started!**\nI will send trade alerts.",
        'bot_stopped': "⏸️ **Bot stopped.**\nAlerts are paused until you restart the bot.",
        
        # Filter
        'filter_menu_title': "⚙️ **Filters & Settings**\n\nSelect minimum trade amount and categories:",
        'filter_section_amount': "➖➖➖ TRADE AMOUNT ➖➖➖",
        'filter_section_category': "➖➖➖ CATEGORIES ➖➖➖",
        'filter_set': "✅ Settings updated!\n\n**Threshold:** ${min:,}\n**Categories:** {categories}",
        'filter_toast': "Settings updated!",
        
        # Settings
        'settings_title': "⚙️ **Category Settings**\n\nSelect which markets to track:",
        'settings_all': "All trades",
        'settings_other': "All except crypto & sports",
        'settings_crypto': "💰 Crypto",
        'settings_sports': "⚽ Sports",
        'settings_done': "✔️ Done",
        'settings_saved': "✅ **Settings saved!**\n\nActive categories: {categories}",
        'settings_toast': "Settings saved!",
        'cat_other': "Other",
        'cat_crypto': "Crypto",
        'cat_sports': "Sports",
        'cat_nothing': "None",
        
        # About
        'about': """*Polymarket Whales* 🐋
Real-time monitoring of large trades on [Polymarket](https://polymarket.com).

*Functionality:*
• Trade alerts from $500 to $100,000+
• Customizable amount threshold
• Category selection (Crypto, Sports, Other)

*Volume classification:*
🔥 MEGA WHALE — >$100,000
⚡ SUPER WHALE — >$50,000
🐋 WHALE — >$25,000
🦈 SHARK — >$10,000
🐬 DOLPHIN — >$5,000
🐟 FISH — >$2,000
🦐 SHRIMP — >$500

*Category definitions:*
1. 💰 *Crypto*
Keywords: bitcoin, btc, ethereum, eth, solana, doge, pepe, binance, nft, airdrop, etc.
2. ⚽ *Sports*
Keywords: nfl, nba, football, soccer, ufc, f1, lakers, goal, etc.

💬 Feedback: @Andrey\_Os

⚡ *Best Exchange to Trade:*
[Join Bybit and get massive bonuses! 🎁](https://www.bybit.com/invite?ref=JDRKDN)""",
        
        # Trade alerts
        'open_market': "Open market",
    }
}

# Trade level names per language
TRADE_LEVELS = {
    'ru': {
        20000: "Кит",
        10000: "Акула",
        5000: "Дельфин",
        1000: "Рыба",
        100: "Креветка",
    },
    'en': {
        20000: "Whale",
        10000: "Shark",
        5000: "Dolphin",
        1000: "Fish",
        100: "Shrimp",
    }
}


def get_text(lang: str, key: str, **kwargs) -> str:
    """Get localized text."""
    text = TRANSLATIONS.get(lang, TRANSLATIONS['ru']).get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except KeyError:
            pass
    return text


def get_trade_level_name(lang: str, min_value: int) -> str:
    """Get localized trade level name."""
    return TRADE_LEVELS.get(lang, TRADE_LEVELS['ru']).get(min_value, "")
