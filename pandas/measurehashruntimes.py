import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

df1_sizes = np.array([100_000, 1_000_000, 3_000_000, 5_000_000, 7_000_000])
df2_sizes = np.array([100_000, 1_000_000, 3_000_000, 5_000_000, 7_000_000])

def generate_data(df1size, df2size):
    data1 = {
        'pkey': np.arange(0, df1size),
        'col2': np.random.randint(100, 1000, size=df1size),
        'col3': np.random.randint(1000, 10000, size=df1size),
        'col4': np.random.randint(10, 50, size=df1size),
        'col5': np.random.randint(1, 500, size=df1size),
        'col6': np.random.randint(5000, 10000, size=df1size),
        'col7': np.random.randint(10000, 50000, size=df1size),
        'col8': np.random.randint(50000, 100000, size=df1size),
        'col9': np.random.randint(0, 2, size=df1size),
        'col10': np.random.randint(-100000, 100000, size=df1size),
        'col11': np.random.randint(-100000, 100000, size=df1size),
        'col12': np.random.randint(-100000, 100000, size=df1size),
        'col13': np.random.randint(-100000, 100000, size=df1size),
        'col14': np.random.randint(-100000, 100000, size=df1size),
        'col15': np.random.randint(-100000, 100000, size=df1size),
    }
    df1 = pd.DataFrame(data1).set_index("pkey")

    data2 = {
        'pkey': np.arange(0, df2size),
        'fkey': np.random.randint(0, df1size, size=df2size),
        'col3': np.random.randint(1000, 10000, size=df2size),
        'col4': np.random.randint(10, 50, size=df2size),
        'col5': np.random.randint(1, 500, size=df2size),
        'col6': np.random.randint(5000, 10000, size=df2size),
        'col7': np.random.randint(10000, 50000, size=df2size),
        'col8': np.random.randint(50000, 100000, size=df2size),
        'col9': np.random.randint(0, 2, size=df2size),
        'col10': np.random.randint(-100000, 100000, size=df2size),
        'col11': np.random.randint(-100000, 100000, size=df2size),
        'col12': np.random.randint(-100000, 100000, size=df2size),
        'col13': np.random.randint(-100000, 100000, size=df2size),
        'col14': np.random.randint(-100000, 100000, size=df2size),
        'col15': np.random.randint(-100000, 100000, size=df2size),
    }
    df2 = pd.DataFrame(data2)
    return df1, df2

def hash_join(df1, df2):
    return df2.merge(df1, left_on="fkey", right_index=True)

def measure_hash_join_runtime(df1_sizes, df2_sizes):
    hash_runtimes = {}

    for df1size in df1_sizes:
        runtimes = []
        for df2size in df2_sizes:
            df1, df2 = generate_data(df1size, df2size)
            start_time = time.time()
            hash_join(df1, df2)
            end_time = time.time()
            runtimes.append(end_time - start_time)
        hash_runtimes[df1size] = runtimes
    return np.array([hash_runtimes[df1_size] for df1_size in df1_sizes])

def model_hash_join(df1sizes, df2sizes):
  hash_runtimes = measure_hash_join_runtime(df1sizes, df2sizes)

  df1_flat = np.repeat(df1sizes, len(df2sizes))
  df2_flat = np.tile(df2sizes, len(df1sizes))
  runtime_flat = np.array(hash_runtimes).flatten()

  # linear model in size of two inputs
  def hash_join_runtime(X, a, b, c):
      df1size, df2size = X
      return a * df1size + b * df2size + c

  coefficients, _ = curve_fit(hash_join_runtime, (df1_flat, df2_flat), runtime_flat)
  a, b, c = coefficients
  print(f"Fitted coefficients for hash join model")
  print("a: ", a)
  print("b: ", b)
  print("c: ", c)

  predicted_runtimes = hash_join_runtime((df1_flat, df2_flat), *coefficients)

  plt.figure(figsize=(10, 6))
  plt.scatter(runtime_flat, predicted_runtimes, label="Observed vs Predicted", color="blue")
  plt.plot([min(runtime_flat), max(runtime_flat)],
          [min(runtime_flat), max(runtime_flat)], color="red", linestyle="--", label="Ideal Fit")
  plt.xlabel("Observed Runtimes")
  plt.ylabel("Predicted Runtimes")
  plt.legend()
  plt.title("Hash Join Runtime Model Fit")
  plt.grid()
  plt.show()

model_hash_join(df1_sizes, df2_sizes)