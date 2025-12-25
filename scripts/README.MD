### so-tag-trends

# Работа парсинга

## Парсинг (ветка `parsing`)

Ветка содержит скрипты, которые выгружают данные из StackExchange API (StackOverflow) по тегам **sql / postgresql / mysql / sqlite** и сохраняют результаты в CSV-файлы.

### Что делает парсинг

1) `parse_questions.py` — получает список вопросов по выбранным тегам и сохраняет их в `csv/questions.csv`.
2) `parse_users.py` — берёт `user_id` из `csv/questions.csv`, догружает данные по пользователям и сохраняет в `csv/users.csv`.
3) `build_days.py` — агрегирует количество вопросов по дням и тегам на основе `csv/questions.csv` и сохраняет в `csv/days.csv`.

### Куда сохраняется результат

Все выходные файлы создаются в папке `csv/`:
- `csv/questions.csv`
- `csv/users.csv`
- `csv/days.csv`

> Важно: скрипты ожидают, что папка `csv/` существует (или создают её автоматически, если в коде есть `mkdir(..., exist_ok=True)`).

### Формат файлов

#### `csv/questions.csv`
Одна строка — один вопрос (или одна пара «вопрос-тег», если вы размножаете вопрос по целевым тегам).

Колонки:
- `id` — id вопроса на StackOverflow.
- `user_id` — id автора (если доступен).
- `tag` — тег из набора (sql/postgresql/mysql/sqlite).
- `title` — заголовок.
- `body` — текст вопроса.
- `upvotes` — апвоуты (может быть пустым, если поле не получено из API).
- `downvotes` — даунвоуты (может быть пустым, если поле не получено из API).
- `created_at` — дата/время создания (ISO).
- `answers_count` — количество ответов.
- `is_answered` — признак, что есть ответ/вопрос отвечен.

#### `csv/users.csv`
Одна строка — один пользователь, который встретился в `questions.csv`.

Колонки:
- `id` — user_id.
- `rating` — рейтинг/репутация.
- `region` — сейчас не заполняется.
- `tz` — сейчас не заполняется (0).

#### `csv/days.csv`
Агрегация по дням и тегам (строится из `questions.csv`).

Колонки:
- `id` — порядковый id записи.
- `date` — `YYYY-MM-DD`.
- `tag` — тег.
- `question_count` — количество вопросов/строк за день для тега.

### Как запустить (Windows PowerShell)

Из корня проекта:

### Запуск (Windows PowerShell)

```py -m venv .venv
..venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python .\scripts\parse_questions.py
python .\scripts\parse_users.py
python .\scripts\build_days.py```


# Настройка среды для запуска

## Настройка окружения (Python .venv) и зависимости

Открой терминал в VS Code: **Terminal → New Terminal**

### 1) Создай виртуальное окружение и установи зависимости

**Windows (PowerShell):**
```py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
undefined```

> Если PowerShell блокирует активацию, можно временно разрешить скрипты:
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

### 2) Выбери интерпретатор Python в VS Code

1. Нажми **Ctrl+Shift+P**
2. Выбери **Python: Select Interpreter**
3. Укажи интерпретатор из виртуального окружения: `.venv\Scripts\python.exe`