import pandas as pd
# --- load aggregated daily table ---
df = pd.read_csv("daily_tag_counts.csv")

# --- prep ---
df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.sort_values(["tag", "date"])
# --- Table B: summary stats per tag ---
table_b = (
    df.groupby("tag")["questions_count"]
      .agg(
          mean_questions="mean",
          variance_questions="var",
          min_questions="min",
          max_questions="max"
      )
      .reset_index()
)

table_b["range_questions"] = table_b["max_questions"] - table_b["min_questions"]
table_b.to_csv("table_B_tag_stats.csv", index=False, encoding="utf-8-sig")
