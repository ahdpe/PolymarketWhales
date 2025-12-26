# Polymarket Whales Bot 🐋

[🇷🇺 Русский](#-русский) | [🇬🇧 English](#-english)

---

## 🇷🇺 Русский

Telegram-бот для отслеживания крупных сделок ("китов") на [Polymarket](https://polymarket.com) в режиме реального времени.

### Возможности

- 📊 **Мониторинг сделок** от $500 до $100,000+
- 💰 **Фильтр по сумме** — выбери минимальный порог
- 📂 **Фильтр по категориям** — Крипто, Спорт, Остальное
- ⚖️ **Фильтр вероятности** — исключает почти решённые рынки (99.9%)
- 🌐 **Двуязычный интерфейс** — Русский / English
- 🔗 **Ссылки на профиль трейдера** и рынок

### Классификация объёмов

| Эмодзи | Уровень | Сумма |
|--------|---------|-------|
| 🔥 | Мега Кит | >$100,000 |
| ⚡ | Супер Кит | >$50,000 |
| 🐋 | Кит | >$25,000 |
| 🦈 | Акула | >$10,000 |
| 🐬 | Дельфин | >$5,000 |
| 🐟 | Рыба | >$2,000 |
| 🦐 | Креветка | >$500 |

### Как это работает

1. **Получение данных:** Опрос Polymarket Data API каждые 3 сек
2. **Агрегация:** Мелкие части крупных ордеров собираются в серии (60 сек окно)
3. **Дедупликация:** SQLite + LRU кэш предотвращают дубли при перезапуске
4. **Уведомления:** Персонализированные алерты в Telegram

### Установка

```bash
git clone https://github.com/ahdpe/PolymarketWhales.git
cd PolymarketWhales
pip install -r requirements.txt
```

Создайте `.env` файл:
```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Запуск:
```bash
python main.py
```

---

## 🇬🇧 English

Telegram bot for real-time tracking of large trades ("whales") on [Polymarket](https://polymarket.com).

### Features

- 📊 **Trade monitoring** from $500 to $100,000+
- 💰 **Amount filter** — choose minimum threshold
- 📂 **Category filter** — Crypto, Sports, Other
- ⚖️ **Probability filter** — excludes near-resolved markets (99.9%)
- 🌐 **Bilingual interface** — Russian / English
- 🔗 **Links to trader profile** and market

### Volume Classification

| Emoji | Level | Amount |
|-------|-------|--------|
| 🔥 | Mega Whale | >$100,000 |
| ⚡ | Super Whale | >$50,000 |
| 🐋 | Whale | >$25,000 |
| 🦈 | Shark | >$10,000 |
| 🐬 | Dolphin | >$5,000 |
| 🐟 | Fish | >$2,000 |
| 🦐 | Shrimp | >$500 |

### How It Works

1. **Data fetching:** Polls Polymarket Data API every 3 sec
2. **Aggregation:** Small fills of large orders are grouped into series (60 sec window)
3. **Deduplication:** SQLite + LRU cache prevents duplicates on restart
4. **Notifications:** Personalized alerts via Telegram

### Installation

```bash
git clone https://github.com/ahdpe/PolymarketWhales.git
cd PolymarketWhales
pip install -r requirements.txt
```

Create `.env` file:
```
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Run:
```bash
python main.py
```

---

## Tech Stack

- **Language:** Python 3.10+
- **Libraries:** aiogram, aiohttp, sqlite3
- **Config:** `.env` (tokens), `user_settings.json` (user preferences)

## License

MIT

## Contact

- Telegram: [@Andrey_Os](https://t.me/Andrey_Os)
- GitHub: [ahdpe/PolymarketWhales](https://github.com/ahdpe/PolymarketWhales)
