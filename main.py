import pandas as pd
import cProfile
import pstats
# import time


df1 = pd.read_parquet("numerical_data1_huge.pq", engine="fastparquet")
df2 = pd.read_parquet("numerical_data2_huge.pq", engine="fastparquet")
df3 = pd.read_parquet("numerical_data3_huge.pq", engine="fastparquet")
# df1 = df1.set_index('pkey')
# df2 = df2.set_index('pkey')
# df3 = df3.set_index('pkey')

def func_to_profile() -> None:
    print("merge")
    # df3 = df2.merge(df1, left_on="fkey", right_on="pkey")


    # SELECT * from df1 as 1
    from pandas.core.reshape.merge import _MergePlan
    # JOIN df2 as 2 on 1.fkey2=2.pkey
    # JOIN df3 as 3 on 1.fkey3=3.pkey
    plan = _MergePlan(df=df1, alias="1", sort=False) \
        .add_merge(df2, left_alias="1", alias="2", left_on="fkey2", right_on="pkey") \
        .add_merge(df3, left_alias="1", alias="3", left_on="fkey3", right_on="pkey")
    df4 = plan.execute_merge_plan()
    print("merged")
    print(df4.head(10))
    print("length")
    print(len(df4))

    # pd.merge_multi(dfs=[df1, df2, df3], ons=[("fkey", "pkey"), ("fkey", "pkey")], suffixes=("_1", "_2", "_3"), sort=True)

if __name__ == "__main__":
    cProfile.run("func_to_profile()", "profile.prof")
    p = pstats.Stats("profile.prof")
    p.sort_stats('time').print_stats(20)
