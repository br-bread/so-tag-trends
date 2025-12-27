import csv
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
import html
from bs4 import BeautifulSoup


OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TAGS = ["sql", "postgresql", "mysql", "sqlite"]

QUESTIONS_API = "https://api.stackexchange.com/2.3/questions"
USERS_API = "https://api.stackexchange.com/2.3/users/"  # /users/{ids}

SITE = "stackoverflow"


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()


def html_to_text(src: str) -> str:
    soup = BeautifulSoup(src or "", "html.parser")
    return soup.get_text(separator="\n", strip=True)


def se_get(url: str, *, params: dict, timeout: int = 30) -> dict:
    """GET + raise + respect backoff (в ответе wrapper object)."""
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    backoff = data.get("backoff")
    if backoff:
        time.sleep(int(backoff))
    return data


def fetch_questions_page(*, tag: str, page: int, fromdate: int, todate: int, pagesize: int = 100) -> dict:
    params = {
        "site": SITE,
        "tagged": tag,
        "sort": "creation",   
        "order": "asc",
        "fromdate": fromdate,
        "todate": todate,
        "page": page,
        "pagesize": pagesize,
        "filter": "!T3AudpctoiK*cxEcjU",
    }
    return se_get(QUESTIONS_API, params=params)


def fetch_users_locations(user_ids: list[int]) -> dict[int, str]:
    """
    Возвращает user_id -> location для списка id.
    {ids} может содержать до 100 id, разделённых ';' [web:153].
    """
    ids = [uid for uid in user_ids if isinstance(uid, int)]
    ids = ids[:100]
    if not ids:
        return {}

    ids_part = ";".join(map(str, ids))
    url = USERS_API + ids_part
    params = {"site": SITE}
    data = se_get(url, params=params)

    out: dict[int, str] = {}
    for u in data.get("items", []):
        uid = u.get("user_id")
        if isinstance(uid, int):
            out[uid] = (u.get("location") or "")  
    return out


def chunked(seq, size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def main(days_back: int = 183, sleep_s: float = 0.2):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_back)
    fromdate = int(start.timestamp())
    todate = int(now.timestamp())

    fieldnames = [
        "id",
        "user_id",
        "location", 
        "tag",
        "title",
        "body",
        "score",
        "up_vote_count",
        "down_vote_count",
        "created_at",
        "answers_count",
        "view_count",
        "is_answered",
    ]

    user_location_cache: dict[int, str] = {}

    with (OUT_DIR / "questions.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for tag in TAGS:
            page = 1
            while True:
                data = fetch_questions_page(tag=tag, page=page, fromdate=fromdate, todate=todate)
                items = data.get("items", [])

                # 1) собрать user_id с текущей страницы
                user_ids_to_fetch = []
                for q in items:
                    owner = q.get("owner") or {}
                    uid = owner.get("user_id")
                    if isinstance(uid, int) and uid not in user_location_cache:
                        user_ids_to_fetch.append(uid)

                # 2) батч-запрос /users/{ids} (до 100 id за раз) [web:153]
                # делаем unique, чтобы не тратить запросы
                user_ids_to_fetch = sorted(set(user_ids_to_fetch))
                for batch in chunked(user_ids_to_fetch, 100):
                    user_location_cache.update(fetch_users_locations(batch))
                    time.sleep(sleep_s)

                for q in items:
                    owner = q.get("owner") or {}
                    uid = owner.get("user_id")  
                    location = user_location_cache.get(uid, "") if isinstance(uid, int) else ""

                    w.writerow({
                        "id": q.get("question_id"),
                        "user_id": uid,
                        "location": location,
                        "tag": tag, 
                        "title": q.get("title", ""),
                        "body": q.get("body", ""),
                        "score": q.get("score", 0),
                        "up_vote_count": q.get("up_vote_count", 0),
                        "down_vote_count": q.get("down_vote_count", 0),
                        "created_at": iso_utc(q.get("creation_date", 0)),
                        "answers_count": q.get("answer_count", 0),
                        "view_count": q.get("view_count", 0),
                        "is_answered": q.get("is_answered", False),
                    })

                if not data.get("has_more"):
                    break

                time.sleep(sleep_s)
                page += 1


if __name__ == "__main__":
    main()
