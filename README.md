# How To Run

First, follow the instructions here to setup and build Pandas:

https://pandas.pydata.org/docs/dev/development/contributing_environment.html

To generate plots comparing hash join and merge join on Pandas, run pandas/perfplotter.py

To generate coefficients for the models of hash join / merge join / sort, run pandas/measureruntimes.py

To generate plots or statistics comparing the performance of the optimal plan with the performance of the original plan, look into main.py

To generate parquet files for the 4 dataframes in the report, run gen_rdb.py

To find our code changes, look in pandas/core/reshape/merge.py starting from line 922



