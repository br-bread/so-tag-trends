import csv
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TAGS = ["sql", "postgresql", "mysql", "sqlite"]
TAGSET = set(TAGS)

API = "https://api.stackexchange.com/2.3/search/advanced"

def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

def fetch_page(page: int, pagesize: int = 100):
    params = {
        "site": "stackoverflow",
        "tagged": ";".join(TAGS),     # хотя бы один из тегов [page:0]
        "order": "desc",
        "sort": "creation",
        "page": page,
        "pagesize": pagesize,         # 0..100 [page:1]
        "filter": "withbody",         # чтобы пришёл body
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main(limit_pages: int = 25, sleep_s: float = 0.2):
    fieldnames = [
        "id", "user_id", "tag", "title", "body",
        "upvotes", "downvotes", "created_at",
        "answers_count", "is_answered",
    ]

    with (OUT_DIR / "questions.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        page = 1
        while page <= limit_pages:
            data = fetch_page(page)
            items = data.get("items", [])

            for q in items:
                q_tags = q.get("tags", [])
                matched = [t for t in q_tags if t in TAGSET]
                if not matched:
                    continue

                owner = q.get("owner") or {}

                for t in matched:
                    w.writerow({
                        "id": q.get("question_id"),
                        "user_id": owner.get("user_id"),
                        "tag": t,
                        "title": q.get("title"),
                        "body": q.get("body", ""),
                        # Эти поля могут быть None без кастомного filter:
                        "upvotes": q.get("up_vote_count"),
                        "downvotes": q.get("down_vote_count"),
                        "created_at": iso_utc(q.get("creation_date", 0)),
                        "answers_count": q.get("answer_count", 0),
                        "is_answered": q.get("is_answered", False),
                    })

            if not data.get("has_more"):   # признак, что страниц больше нет [page:1]
                break

            time.sleep(sleep_s)
            page += 1

if __name__ == "__main__":
    main()
