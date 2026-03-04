import pandas as pd

df = pd.read_parquet('./ML/test_data', engine='pyarrow')

# Sort by time
df = df.sort_values(by=['match_id', 'window_start'])

# Basic dataset overview
print("=== DATASET OVERVIEW ===")
print(f"Number of 5-minute windows: {len(df)}")
print(f"Number of unique matches: {df['match_id'].nunique()}\n")

# Basic statistics for key features
key_features = [
    'match_id', 'current_minute', 'current_second', 'home_team', 'away_team', 
    'home_goals', 'away_goals', 'momentum_home_xg', 'momentum_home_possession'
]

# Identify what half each window belongs to based on minute progression
df['minute_diff'] = df.groupby('match_id')['current_minute'].diff()
df['is_second_half'] = (df['minute_diff'] < 0).groupby(df['match_id']).cumsum().astype(bool)

df['half'] = df['is_second_half'].apply(lambda x: 2 if x else 1)

df.drop(columns=['minute_diff', 'is_second_half'], inplace=True)

print("=== SAMPLE DATA ===")
print(df[key_features])

with open('./ML/test_output.txt', 'w') as f:
    f.write("=== DATASET SIZE ===\n")
    f.write(f"Number of 5-minute windows: {len(df)}\n")
    f.write(f"Number of unique matches: {df['match_id'].nunique()}\n\n")

    f.write("=== SAMPLE DATA ===\n")
    f.write(df.to_string(index=False))