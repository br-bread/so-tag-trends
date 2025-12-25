import csv
from datetime import datetime
from pathlib import Path

OUT_DIR = Path("csv")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def to_date(created_at: str) -> str:
    # created_at в ISO, берём YYYY-MM-DD
    return created_at[:10]

def main():
    counts = {}  # (date, tag) -> count

    with (OUT_DIR / "questions.csv").open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            d = to_date(row["created_at"])
            t = row["tag"]
            counts[(d, t)] = counts.get((d, t), 0) + 1

    with (OUT_DIR / "data.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "date", "tag", "question_count"])
        w.writeheader()

        i = 1
        for (d, t), c in sorted(counts.items()):
            w.writerow({"id": i, "date": d, "tag": t, "question_count": c})
            i += 1

if __name__ == "__main__":
    main()