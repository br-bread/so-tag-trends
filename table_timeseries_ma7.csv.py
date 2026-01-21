import pandas as pd
# --- load aggregated daily table ---

df = pd.read_csv("daily_tag_counts.csv")

# --- prep ---
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.sort_values(["tag", "date"])

# --- Table A: time series + MA7 ---
df["ma7"] = (
    df.groupby("tag")["questions_count"]
      .transform(lambda s: s.rolling(window=7, min_periods=7).mean())
)

table_a = df[["date", "tag", "questions_count", "ma7"]].copy()
table_a.to_csv("table_A_timeseries_ma7.csv", index=False, encoding="utf-8-sig")