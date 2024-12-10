import pandas as pd
import cProfile
import pstats
import numpy as np
import matplotlib.pyplot as plt
# import time


df1 = pd.read_parquet("numerical_data1_huge.pq", engine="fastparquet")
df2 = pd.read_parquet("numerical_data2_huge.pq", engine="fastparquet")
df3 = pd.read_parquet("numerical_data3_huge.pq", engine="fastparquet")
df4 = pd.read_parquet("numerical_data4_huge.pq", engine="fastparquet")
df1 = df1.set_index('pkey')
df2 = df2.set_index('pkey')
df3 = df3.set_index('pkey')
df4 = df4.set_index('pkey')
from pandas.core.reshape.merge import _MergePlan
def func_to_profile(opt) -> None:
    #print("merge")
    # df3 = df2.merge(df1, left_on="fkey", right_on="pkey")


    # SELECT * from df1 as 1
   # from pandas.core.reshape.merge import _MergePlan
    # JOIN df2 as 2 on 1.fkey2=2.pkey
    # JOIN df3 as 3 on 1.fkey3=3.pkey
    # plan = _MergePlan(df1, alias="1", sort=False) \
    #     .add_merge(df2, left_alias="1", alias="2", left_on="fkey2", right_on="pkey") \
    #     .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey") \
    #     .add_merge(df4, left_alias="2", alias="4", left_on="fkey", right_on="pkey")
    plan = _MergePlan(df2, alias="2", sort=False) \
        .add_merge(df4, left_alias="2", alias="4", left_on="fkey", right_on="pkey") \
        .add_merge(df1, left_alias="2", alias="1", left_on="pkey", right_on="fkey2") \
        .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey")
    # plan = _MergePlan(df1, alias="1", sort=False) \
    #     .add_merge(df2, left_alias="1", alias="2", left_on="fkey2", right_on="pkey") \
    #     .add_merge(df4, left_alias="2", alias="4", left_on="fkey", right_on="pkey") \
    #     .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey")

    
    df5 = plan.execute_merge_plan(optimize = opt)
    # print("merged")
    # print(df5.head(10))
    #df5.info()

    # pd.merge_multi(dfs=[df1, df2, df3], ons=[("fkey", "pkey"), ("fkey", "pkey")], suffixes=("_1", "_2", "_3"), sort=True)

df9 = df1.merge(df2, left_on="fkey2", right_on="pkey")
def func_to_profile_2() -> None:
    print("merge2")
    # df8 = df9.merge(df3, left_on="fkey3", right_on="pkey", suffixes=("_1", "_2"))
    # df7 = df8.merge(df4, left_on="fkey_1", right_on="pkey", suffixes=("_i", "_ii"))
    print(df9["col3_y"].agg(['count', 'size', 'nunique']))
    print(df9["col3_y"].head(10))

    print("merged")
    # print(df7.head(10))
    # df7.info()
    # print("length")
    # print(len(df7))

def plot_results():
    from gen_rdb import generate_data
    rows2_values = [5_000, 10_000, 50_000, 100_000, 500_000, 750_000][::-1]
    original_runtimes = []
    new_runtimes = []
    for rows2 in rows2_values:
        print(rows2)
        df1, df2, df3, df4 = generate_data(rows1 = 1_000_000, rows2=rows2)
        df1 = df1.set_index('pkey')
        df2 = df2.set_index('pkey')
        df3 = df3.set_index('pkey')
        df4 = df4.set_index('pkey')
        new_runtime = []
        iterations = 30
        for i in range(iterations):
            plan = _MergePlan(df2, alias="2", sort=False) \
            .add_merge(df4, left_alias="2", alias="4", left_on="fkey", right_on="pkey") \
            .add_merge(df1, left_alias="2", alias="1", left_on="pkey", right_on="fkey2") \
            .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey")
            start_time = time.time()
            df5 = plan.execute_merge_plan(optimize = True)
            end_time = time.time()
            new_runtime.append(end_time - start_time)
        new_runtimes.append(np.mean(new_runtime))

        original_runtime = []
        for i in range(iterations):
            plan = _MergePlan(df2, alias="2", sort=False) \
            .add_merge(df4, left_alias="2", alias="4", left_on="fkey", right_on="pkey") \
            .add_merge(df1, left_alias="2", alias="1", left_on="pkey", right_on="fkey2") \
            .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey")
            start_time = time.time()
            df5 = plan.execute_merge_plan(optimize = False)
            end_time = time.time()
            original_runtime.append(end_time - start_time)
        original_runtimes.append(np.mean(original_runtime))
    
    plt.figure(figsize=(10, 6))
    plt.plot(rows2_values[::-1], original_runtimes[::-1], label="Original Plan Runtimes", marker='o')
    plt.plot(rows2_values[::-1], new_runtimes[::-1], label="Optimized Plan Runtimes", marker='o')
    plt.xlabel("Number of Rows (rows2)")
    plt.ylabel("Runtime (seconds)")
    plt.title("Original vs Optimized Plan Runtimes")
    plt.legend()
    plt.grid(True)
    plt.xscale("log")
    plt.show()



import time
if __name__ == "__main__":
    # func_to_profile()
    plot_results()
    # original_runtimes = []
    # new_runtimes = []
    # num_iterations = 50
    # for i in range(num_iterations):
    #     start_time = time.time()
    #     func_to_profile(False)
    #     end_time = time.time()
    #     original_runtimes.append(end_time - start_time)

    #     start_time = time.time()
    #     func_to_profile(True)
    #     end_time = time.time()
    #     new_runtimes.append(end_time - start_time)

    # runtimes = original_runtimes
    # avg_runtime = np.mean(runtimes)
    # std_runtime = np.std(runtimes)
    # min_runtime = np.min(runtimes)
    # max_runtime = np.max(runtimes)

    # print("\n=== Runtime Statistics for Original Merge Plan===")
    # print(f"Num Iterations: {num_iterations}")
    # print(f"Average Runtime: {avg_runtime:.4f} seconds")
    # print(f"Standard Deviation: {std_runtime:.4f} seconds")
    # print(f"Minimum Runtime: {min_runtime:.4f} seconds")
    # print(f"Maximum Runtime: {max_runtime:.4f} seconds")

    # runtimes = new_runtimes
    # avg_runtime = np.mean(runtimes)
    # std_runtime = np.std(runtimes)
    # min_runtime = np.min(runtimes)
    # max_runtime = np.max(runtimes)

    # print("\n=== Runtime Statistics for Optimized Merge Plan===")
    # print(f"Num Iterations: {num_iterations}")
    # print(f"Average Runtime: {avg_runtime:.4f} seconds")
    # print(f"Standard Deviation: {std_runtime:.4f} seconds")
    # print(f"Minimum Runtime: {min_runtime:.4f} seconds")
    # print(f"Maximum Runtime: {max_runtime:.4f} seconds")


    # cProfile.run("func_to_profile()", "profile.prof")
    # p = pstats.Stats("profile.prof")
    # p.sort_stats('time').print_stats(20)
