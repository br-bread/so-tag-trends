### so-tag-trends

# Настройка среды для запуска

## Настройка окружения (Python .venv) и зависимости

Открой терминал в VS Code: **Terminal → New Terminal**

### 1) Создай виртуальное окружение и установи зависимости

**Windows (PowerShell):**
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

> Если PowerShell блокирует активацию, можно временно разрешить скрипты:
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

### 2) Выбери интерпретатор Python в VS Code

1. Нажми **Ctrl+Shift+P**
2. Выбери **Python: Select Interpreter**
3. Укажи интерпретатор из виртуального окружения:
   - `.venv\Scripts\python.exe`

### 3) Запусти скрипты

В терминале (с активированным `.venv`):

python .\scripts\parse_questions.py
python .\scripts\parse_users.py
python .\scripts\build_days.py