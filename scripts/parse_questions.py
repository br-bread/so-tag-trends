import csv
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TAGS = ["sql", "postgresql", "mysql", "sqlite"]

API = "https://api.stackexchange.com/2.3/questions"


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def fetch_page(*, tag: str, page: int, fromdate: int, todate: int, pagesize: int = 100):
    params = {
        "site": "stackoverflow",
        "tagged": tag,           # /questions: tagged — AND, поэтому 1 тег за запрос [web:126]
        "sort": "creation",      # creation_date [web:126]
        "order": "asc",
        "fromdate": fromdate,
        "todate": todate,
        "page": page,
        "pagesize": pagesize,
        "filter": "withbody",
    }
    r = requests.get(API, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main(days_back: int = 183, sleep_s: float = 0.2):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days_back)
    fromdate = int(start.timestamp())
    todate = int(now.timestamp())

    print("UTC window:", start.isoformat(), "->", now.isoformat())
    print("epoch window:", fromdate, "->", todate)

    fieldnames = [
        "id", "user_id", "tag", "title", "body",
        "created_at", "answers_count", "is_answered",
    ]

    total_rows = 0

    with (OUT_DIR / "questions.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for tag in TAGS:
            page = 1
            while True:
                data = fetch_page(tag=tag, page=page, fromdate=fromdate, todate=todate)
                items = data.get("items", [])

                print(f"tag={tag} page={page} items={len(items)} has_more={data.get('has_more')}")

                for q in items:
                    owner = q.get("owner") or {}
                    w.writerow({
                        "id": q.get("question_id"),
                        "user_id": owner.get("user_id"),
                        "tag": tag,  # тут именно тот тег, по которому качали
                        "title": q.get("title"),
                        "body": q.get("body", ""),
                        "created_at": iso_utc(q.get("creation_date", 0)),
                        "answers_count": q.get("answer_count", 0),
                        "is_answered": q.get("is_answered", False),
                    })
                    total_rows += 1

                backoff = data.get("backoff")
                if backoff:
                    time.sleep(int(backoff))

                if not data.get("has_more"):
                    break

                time.sleep(sleep_s)
                page += 1

    print("written rows:", total_rows)


if __name__ == "__main__":
    main()
