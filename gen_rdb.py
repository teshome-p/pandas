import numpy as np
import pandas as pd

rows1 = 5000000
rows2 = 1000000
rows3 =   60000
rows4 =   6

data1 = {
  'pkey': np.arange(0, rows1),
  'fkey2': np.random.randint(0, rows2, size=rows1),
  'fkey3': np.random.randint(0, rows3, size=rows1),
  'col2': np.random.randint(100, 1000, size=rows1),
  'col3': np.random.randint(1000, 10000, size=rows1),
  'col4': np.random.randint(10, 50, size=rows1),
  'col5': np.random.randint(1, 500, size=rows1),
  'col6': np.random.randint(5000, 10000, size=rows1),
  'col7': np.random.randint(10000, 50000, size=rows1),
  'col8': np.random.randint(50000, 100000, size=rows1),
  'col9': np.random.randint(0, 2, size=rows1),
  'col10': np.random.randint(-100000, 100000, size=rows1),
  'col11': np.random.randint(-100000, 100000, size=rows1),
  'col12': np.random.randint(-100000, 100000, size=rows1),
  'col13': np.random.randint(-100000, 100000, size=rows1),
  'col14': np.random.randint(-100000, 100000, size=rows1),
  'col15': np.random.randint(-100000, 100000, size=rows1)
}
df1 = pd.DataFrame(data1)
df1.to_parquet("numerical_data1_huge.pq", index=False)

data2 = {
  'pkey': np.arange(0, rows2),
  'fkey': np.random.randint(0, rows4, size=rows2),
  'col3': np.random.randint(1000, 10000, size=rows2),
  'col4': np.random.randint(10, 50, size=rows2),
  'col5': np.random.randint(1, 500, size=rows2),
  'col6': np.random.randint(5000, 10000, size=rows2),
  'col7': np.random.randint(10000, 50000, size=rows2),
  'col8': np.random.randint(50000, 100000, size=rows2),
  'col9': np.random.randint(0, 2, size=rows2),
  'col10': np.random.randint(-100000, 100000, size=rows2),
  'col11': np.random.randint(-100000, 100000, size=rows2),
  'col12': np.random.randint(-100000, 100000, size=rows2),
  'col13': np.random.randint(-100000, 100000, size=rows2),
  'col14': np.random.randint(-100000, 100000, size=rows2),
  'col15': np.random.randint(-100000, 100000, size=rows2)
}

df2 = pd.DataFrame(data2)
df2.to_parquet("numerical_data2_huge.pq", index=False)

data3 = {
  'pkey': np.arange(0, rows3),
  'fkey': np.random.randint(0, 1000, size=rows3),
  'col3': np.random.randint(1000, 10000, size=rows3),
  'col4': np.random.randint(10, 50, size=rows3),
  'col5': np.random.randint(1, 500, size=rows3),
  'col6': np.random.randint(5000, 10000, size=rows3),
  'col7': np.random.randint(10000, 50000, size=rows3),
  'col8': np.random.randint(50000, 100000, size=rows3),
  'col9': np.random.randint(0, 2, size=rows3),
  'col10': np.random.randint(-100000, 100000, size=rows3),
  'col11': np.random.randint(-100000, 100000, size=rows3),
  'col12': np.random.randint(-100000, 100000, size=rows3),
  'col13': np.random.randint(-100000, 100000, size=rows3),
  'col14': np.random.randint(-100000, 100000, size=rows3),
  'col15': np.random.randint(-100000, 100000, size=rows3)
}

df3 = pd.DataFrame(data3)
df3.to_parquet("numerical_data3_huge.pq", index=False)

data4 = {
  'pkey': np.arange(0, rows4),
  'fkey': np.random.randint(0, rows1, size=rows4),
  'col3': np.random.randint(1000, 10000, size=rows4),
  'col4': np.random.randint(10, 50, size=rows4),
  'col5': np.random.randint(1, 500, size=rows4),
  'col6': np.random.randint(5000, 10000, size=rows4),
  'col7': np.random.randint(10000, 50000, size=rows4),
  'col8': np.random.randint(50000, 100000, size=rows4),
}

df4 = pd.DataFrame(data4)
df4.to_parquet("numerical_data4_huge.pq", index=False)
