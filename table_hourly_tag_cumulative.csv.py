import pandas as pd

# load hourly profile
df = pd.read_csv("table_hourly_tag_profile.csv")

# keep only needed columns
df = df[["tag", "hour", "mean_questions"]].copy()

# sort for correct cumulative sum
df = df.sort_values(["tag", "hour"])

# cumulative mean questions within day, per tag
df["cum_mean_questions"] = df.groupby("tag")["mean_questions"].cumsum()

# save
out_path = "table_hourly_tag_cumulative.csv"
df.to_csv(out_path, index=False, encoding="utf-8-sig")

print("Saved:", out_path)
print(df.head(12).to_string(index=False))