import csv
import time
import requests
from pathlib import Path

OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

API = "https://api.stackexchange.com/2.3/users"

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def read_user_ids(questions_csv="questions.csv"):
    ids = set()
    with (OUT_DIR / "questions.csv").open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            uid = row.get("user_id")
            if uid:
                ids.add(uid)
    return sorted(ids, key=int)

def fetch_users_batch(ids):
    url = f"{API}/" + ";".join(ids)   # важно: ';' [web:205]
    params = {"site": "stackoverflow"}
    r = requests.get(url, params=params, timeout=30)

    if r.status_code == 400:
        raise RuntimeError(f"400 Bad Request: {r.text}")  # детали ошибки [web:191]

    r.raise_for_status()
    return r.json().get("items", [])


def main(batch_size: int = 40, sleep_s: float = 0.3):
    user_ids = read_user_ids()

    with (OUT_DIR / "users.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "rating", "region", "tz"])
        w.writeheader()

        for batch in chunks(user_ids, batch_size):
            items = fetch_users_batch(batch)
            for u in items:
                w.writerow({
                    "id": u.get("user_id"),
                    "rating": u.get("reputation"),
                    "region": None,
                    "tz": 0,
                })
            time.sleep(sleep_s)

if __name__ == "__main__":
    main()
