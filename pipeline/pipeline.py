# import sys

# print("arguments", sys.argv)

# if len(sys.argv) < 2:
#     print("Usage: python pipeline.py <month>")
#     sys.exit(1)

# month = int(sys.argv[1])
# print(f"hello pipeline, month={month}")
# import pandas as pd

# df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
# print(df.head())

# df.to_parquet(f"output_{month}.parquet")

import sys
import pandas as pd

print("arguments", sys.argv)

day = int(sys.argv[1])
print(f"Running pipeline for day {day}")


df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")
