import numpy as np
import pandas as pd
import perfplot

sizes = [10_000, 100_000, 1_000_000, 3_000_000, 5_000_000, 7_000_000, 9_000_000]
df1_sizes = sizes
df2_sizes = sizes

def plot_hash_vs_merge(df2_sizes, df1size):
    def generate_data(df2size):
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
        df2_sorted = df2.sort_values(by='fkey')
        return df1, df2, df2_sorted

    def hash_join(df1, df2, _):
        return df2.merge(df1, left_on="fkey", right_index=True)

    def merge_join(df1, _, df2_sorted):
        return df2_sorted.merge(df1, left_on="fkey", right_index=True)

    perfplot.save(
        setup=lambda df2size: generate_data(df2size),
        kernels=[hash_join, merge_join],
        labels=["Hash Join", "Merge Join"],
        n_range=df2_sizes,
        xlabel="Number of rows in df2",
        title=f"Hash Join vs Merge Join Performance dfsize1 = {df1size} ",
        equality_check=False,
        filename=f"hash_vs_merge_df1_{df1size}.png"
    )

for df1size in [1_000]:
    plot_hash_vs_merge(df2_sizes, df1size)