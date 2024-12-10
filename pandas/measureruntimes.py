import numpy as np
import pandas as pd
import time
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

df1_sizes = np.array([100_000, 1_000_000, 3_000_000, 5_000_000, 6_000_000])
df2_sizes = np.array([100_000, 1_000_000, 3_000_000, 5_000_000, 6_000_000])

def generate_data(df1size, df2size, generate_first = True):
    if generate_first: # if generating data to measure sort runtime, don't need this first dataframe
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
    else:
        df1 = None
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

def join(df1, df2):
    return df2.merge(df1, left_on="fkey", right_index=True)

def measure_hash_join_runtime(df1_sizes, df2_sizes):
    hash_runtimes = {}

    for df1size in df1_sizes:
        runtimes = []
        for df2size in df2_sizes:
            df1, df2 = generate_data(df1size, df2size)
            start_time = time.time()
            join(df1, df2)
            end_time = time.time()
            runtimes.append(end_time - start_time)
        hash_runtimes[df1size] = runtimes
    return np.array([hash_runtimes[df1_size] for df1_size in df1_sizes])

def measure_merge_join_runtime(df1_sizes, df2_sizes):
    merge_runtimes = {}

    for df1size in df1_sizes:
        runtimes = []
        for df2size in df2_sizes:
            df1, df2 = generate_data(df1size, df2size)
            df2 = df2.sort_values(by="fkey")
            start_time = time.time()
            join(df1, df2)
            end_time = time.time()
            runtimes.append(end_time - start_time)
        merge_runtimes[df1size] = runtimes
    return np.array([merge_runtimes[df1_size] for df1_size in df1_sizes])

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

def model_merge_join(df1sizes, df2sizes):
  join_runtimes = measure_merge_join_runtime(df1sizes, df2sizes)

  df1_flat = np.repeat(df1sizes, len(df2sizes))
  df2_flat = np.tile(df2sizes, len(df1sizes))
  runtime_flat = np.array(join_runtimes).flatten()

  # linear model in size of two inputs
  def merge_join_runtime(X, a, b, c):
      df1size, df2size = X
      return a * df1size + b * df2size + c

  coefficients, _ = curve_fit(merge_join_runtime, (df1_flat, df2_flat), runtime_flat)
  a, b, c = coefficients
  print(f"Fitted coefficients for merge join model")
  print("a: ", a)
  print("b: ", b)
  print("c: ", c)

  predicted_runtimes = merge_join_runtime((df1_flat, df2_flat), *coefficients)

  plt.figure(figsize=(10, 6))
  plt.scatter(runtime_flat, predicted_runtimes, label="Observed vs Predicted", color="blue")
  plt.plot([min(runtime_flat), max(runtime_flat)],
          [min(runtime_flat), max(runtime_flat)], color="red", linestyle="--", label="Ideal Fit")
  plt.xlabel("Observed Runtimes")
  plt.ylabel("Predicted Runtimes")
  plt.legend()
  plt.title("Merge Join Runtime Model Fit")
  plt.grid()
  plt.show()

# model_hash_join(df1_sizes, df2_sizes)
# model_merge_join(df1_sizes, df2_sizes)

sizes = np.array([10_000, 50_000, 100_000, 250_000, 500_000, 750_000, 1_000_000, 1_500_000, 2_000_000, 2_500_000, 3_000_000, 3_500_000, 4_000_000, 4_500_000, 5_000_000, 5_500_000, 6_000_000, 6_500_000, 7_000_000, 7_500_000, 8_000_000, 8_500_000, 9_000_000, 9_500_000, 10_000_000])

def measure_sort_runtimes(sizes):
    runtimes = []
    for size in sizes:
        _, df = generate_data(size, size, generate_first = False)
        start_time = time.time()
        df.sort_values(by="fkey")
        end_time = time.time()
        runtimes.append(end_time - start_time)
    return np.array(runtimes)

def model_sort(sizes):
    runtimes = measure_sort_runtimes(sizes)

    def sort_runtime_model(n, a, b):
        return a * n * np.log(n) + b

    coefficients, _ = curve_fit(sort_runtime_model, sizes, runtimes)
    a, b = coefficients
    print("Fitted coefficient for sort model")
    print("a: ", a)
    print("b: ", b)

    predicted_runtimes = sort_runtime_model(sizes, a, b)

    plt.figure(figsize=(10, 6))
    plt.scatter(sizes, runtimes, label="Observed Runtimes", color="blue")
    plt.plot(sizes, predicted_runtimes, label="Fitted Runtime Model", color="red", linestyle="--")
    plt.xlabel("Number of Rows (n)")
    plt.ylabel("Sort Runtime (seconds)")
    plt.legend()
    plt.title("Sorting Runtime Model Fit")
    plt.grid()
    plt.show()

model_sort(sizes)