import pandas as pd
import matplotlib.pyplot as plt
import json
import os

month_averages = {}

for p in range(4):
    data = {}  
    path = f"/files/partition-{p}.json"
    if os.path.exists(path):
        with open(path, 'r') as file:
            data = json.load(file)
    for month in ["January", "February", "March"]:
        if month in data:
            latest_year = max(data[month].keys())
            temp = data[month][latest_year]["avg"]
            month_averages[f"{month}-{latest_year}"] = temp

month_series = pd.Series(month_averages)
fig, ax = plt.subplots()
month_series.plot.bar(ax=ax)
ax.set_ylabel('Avg Max Temperature')
plt.title('Month Averages')
ax.set_xticklabels(month_series.index, rotation=45)  
plt.savefig("/files/month.svg")
