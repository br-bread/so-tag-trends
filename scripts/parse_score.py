import csv
from pathlib import Path


IN_CSV = Path("csv") / "questions.csv"
OUT_CSV = Path("csv") / "score.csv"


def main():
    with IN_CSV.open(newline="", encoding="utf-8") as fin, \
         OUT_CSV.open("w", newline="", encoding="utf-8") as fout:

        r = csv.DictReader(fin)

        fieldnames = [
            "id",
            "score",
            "up_vote_count",
            "down_vote_count",
            "answers_count",
            "view_count",
            "is_answered",
        ]
        w = csv.DictWriter(fout, fieldnames=fieldnames)
        w.writeheader() 

        for row in r:
            w.writerow({
                "id": row.get("id"),
                "score": row.get("score"),
                "up_vote_count": row.get("up_vote_count"),
                "down_vote_count": row.get("down_vote_count"),
                "answers_count": row.get("answers_count"),
                "view_count": row.get("view_count"),
                "is_answered": row.get("is_answered"),
            })


if __name__ == "__main__":
    main()
