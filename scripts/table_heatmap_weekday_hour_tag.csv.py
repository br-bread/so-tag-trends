import pandas as pd

questions_path = "C:/Users/Moriarty/Downloads/questions.csv"

# 1) load
questions_df = pd.read_csv(questions_path)

# 2) datetime
questions_df["created_at"] = pd.to_datetime(questions_df["created_at"], errors="coerce", utc=True)

# 3) time features
questions_df["date"] = questions_df["created_at"].dt.date
questions_df["hour"] = questions_df["created_at"].dt.hour
questions_df["weekday"] = questions_df["created_at"].dt.day_name()

# 4) dedupe per (question, tag)
q = questions_df.drop_duplicates(subset=["id", "tag"])

# helper: min non-zero (if exists), else 0
def min_nonzero(series):
    s = series[series > 0]
    return int(s.min()) if len(s) else 0

# -----------------------------------------
# A) Base counts: date x hour x tag
# -----------------------------------------
day_hour_tag = (
    q.groupby(["date", "hour", "tag"])
     .size()
     .reset_index(name="questions_count")
)

# Fill missing (date, hour, tag) with 0 to keep full 24h grid per day per tag
all_dates = sorted(day_hour_tag["date"].unique())
all_hours = list(range(24))
all_tags = sorted(day_hour_tag["tag"].unique())

full_index = pd.MultiIndex.from_product(
    [all_dates, all_hours, all_tags],
    names=["date", "hour", "tag"]
)

day_hour_tag_full = (
    day_hour_tag.set_index(["date", "hour", "tag"])
                .reindex(full_index, fill_value=0)
                .reset_index()
                .sort_values(["tag", "date", "hour"])
)

day_hour_tag_full.to_csv(
    "table_day_hour_tag_counts.csv",
    index=False,
    encoding="utf-8-sig"
)


weekday_hour_tag = (
    q.groupby(["weekday", "hour", "tag"])
     .size()
     .reset_index(name="questions_count")
)

weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
weekday_hour_tag["weekday"] = pd.Categorical(
    weekday_hour_tag["weekday"],
    categories=weekday_order,
    ordered=True
)

weekday_hour_tag = weekday_hour_tag.sort_values(["tag", "weekday", "hour"])

weekday_hour_tag.to_csv(
    "table_heatmap_weekday_hour_tag.csv",
    index=False,
    encoding="utf-8-sig"
)
