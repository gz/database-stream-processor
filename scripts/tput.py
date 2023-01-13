import csv
import pandas as pd


df = pd.read_csv('foo.csv')

df['elapsed_ms'] = df['elapsed_ms'] / 1000
df['elapsed_ms'] = df['elapsed_ms'].round()

df = df.groupby('elapsed_ms').agg({'num_events': 'sum'}).reset_index()
df['tput'] = df['num_events'] / 1

print(df.to_markdown())
