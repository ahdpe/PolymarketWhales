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

### Принцип работы

#### 1. Получение данных (PolymarketService)
- **Источник:** Бот использует публичный **Polymarket Data API** (`data-api.polymarket.com`).
- **Метод:** Бот **опрашивает (polling)** API каждые **3 секунды**.
- **Фильтрация на входе:** Запрашиваются только сделки типа `CASH` на сумму от **$10** (чтобы захватить даже мелкие части крупных ордеров).

#### 2. Обработка и Агрегация (Aggregation)
Одна крупная сделка на Polymarket часто разбивается на множество мелких исполнений (fills). Чтобы не спамить уведомлениями о каждой части, бот собирает их в серии.
- **Группировка:** Сделки объединяются в серию, если совпадают:
  - Кошелек трейдера
  - Рынок (Condition ID)
  - Сторона (BUY/SELL)
  - Исход (YES/NO/Outcome Index)
- **Окно времени:** Сделки собираются в течение **60 секунд** с момента первой части.
- **Порог срабатывания:** Если сумма серии превышает **$500**, она считается значимой и передается на отправку.

#### 3. Дедупликация и Хранение (Persistence)
Чтобы избежать повторных уведомлений (например, при перезапуске):
- **База данных:** Используется локальная база **SQLite** (`data/trades.db`), где хранятся уникальные ключи всех обработанных сделок.
- **Кэш:** В оперативной памяти держится список последних 10,000 сделок (LRU Cache) для мгновенной проверки.
- **Очистка:** Старые записи (старше 72 часов) автоматически удаляются из базы.

#### 4. Telegram Бот (TelegramService)
Бот взаимодействует с пользователями и рассылает уведомления.
- **Персонализация:** Каждый пользователь может настроить свои фильтры:
  - **Минимальная сумма:** от $500 до $100,000
  - **Категории:** Крипто, Спорт, Остальное (определяются по ключевым словам)
  - **Вероятность:** Любая, 1%-99%, 5%-95%, 10%-90%
  - **Язык:** Русский или Английский
- **Интерфейс:**
  - `💰 Сумма сделки` — выбор минимального порога
  - `📂 Категории` — выбор категорий рынков
  - `⚖️ Вероятность` — фильтр по вероятности
  - `▶️ Запустить / ⏸️ Остановить` — переключатель уведомлений
- **Уведомления:** Присылает сообщение с:
  - Эмодзи категории (💰, ⚽, 📌) и названием рынка
  - Типом сделки (Покупка/Продажа) и ценой
  - Суммой сделки (для серий пишет "Series X fills")
  - Уровнем "кита" и ссылкой на трейдера

#### 5. Администрирование
- `/stats` — статистика бота (только для владельца)
- `/users` — список пользователей
- `/broadcast <сообщение>` — рассылка всем пользователям

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

#### 1. Data Fetching (PolymarketService)
- **Source:** Uses public **Polymarket Data API** (`data-api.polymarket.com`).
- **Method:** Polls the API every **3 seconds**.
- **Input filtering:** Only `CASH` type trades from **$10** are requested (to capture small parts of large orders).

#### 2. Processing and Aggregation
A single large trade on Polymarket is often split into multiple small fills. To avoid spamming notifications, the bot groups them into series.
- **Grouping:** Trades are combined into a series if they match:
  - Trader wallet
  - Market (Condition ID)
  - Side (BUY/SELL)
  - Outcome (YES/NO/Outcome Index)
- **Time window:** Trades are collected within **60 seconds** from the first fill.
- **Trigger threshold:** If the series sum exceeds **$500**, it's considered significant.

#### 3. Deduplication and Persistence
To avoid duplicate notifications (e.g., on restart):
- **Database:** Local **SQLite** database (`data/trades.db`) stores unique keys of all processed trades.
- **Cache:** 10,000 most recent trades are kept in memory (LRU Cache) for instant lookup.
- **Cleanup:** Old records (older than 72 hours) are automatically deleted.

#### 4. Telegram Bot (TelegramService)
The bot interacts with users and sends notifications.
- **Personalization:** Each user can configure their filters:
  - **Minimum amount:** from $500 to $100,000
  - **Categories:** Crypto, Sports, Other (determined by keywords)
  - **Probability:** Any, 1%-99%, 5%-95%, 10%-90%
  - **Language:** Russian or English
- **Interface:**
  - `💰 Trade Amount` — select minimum threshold
  - `📂 Categories` — select market categories
  - `⚖️ Probability` — probability filter
  - `▶️ Start / ⏸️ Stop` — notification toggle
- **Notifications:** Sends message with:
  - Category emoji (💰, ⚽, 📌) and market name
  - Trade type (Buy/Sell) and price
  - Trade amount (for series shows "Series X fills")
  - Whale level and link to trader

#### 5. Administration
- `/stats` — bot statistics (owner only)
- `/users` — user list
- `/broadcast <message>` — broadcast to all users

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
