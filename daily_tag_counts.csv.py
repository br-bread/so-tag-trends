import pandas as pd

questions_path = "C:/Users/Moriarty/Downloads/questions.csv"

# 1) load
questions_df = pd.read_csv(questions_path)

# 2) datetime
questions_df["created_at"] = pd.to_datetime(questions_df["created_at"], errors="coerce", utc=True)

# 3) date
questions_df["date"] = questions_df["created_at"].dt.date

# 4) dedupe per (question, tag)
q = questions_df.drop_duplicates(subset=["id", "tag"])

# 5) aggregate daily counts by tag
daily_tag_counts = (
    q.groupby(["date", "tag"])
     .size()
     .reset_index(name="questions_count")
     .sort_values(["date", "tag"])
)

# 6) fill missing dates with 0 for each tag (важно для MA/трендов)
all_dates = pd.date_range(
    pd.to_datetime(daily_tag_counts["date"]).min(),
    pd.to_datetime(daily_tag_counts["date"]).max(),
    freq="D"
).date

all_tags = sorted(daily_tag_counts["tag"].unique())

full_index = pd.MultiIndex.from_product([all_dates, all_tags], names=["date", "tag"])
daily_tag_counts_full = (
    daily_tag_counts.set_index(["date", "tag"])
    .reindex(full_index, fill_value=0)
    .reset_index()
    .sort_values(["date", "tag"])
)

# 7) save
out_path = "daily_tag_counts.csv"
daily_tag_counts_full.to_csv(out_path, index=False, encoding="utf-8-sig")

print("Saved:", out_path)
print(daily_tag_counts_full.head(12).to_string(index=False))
