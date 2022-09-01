# microVelocity

Files from Bachelor Thesis should be run in the following order:

1. BalanceCalc.py
3. SupplyCalc.py
4. microVelocityCalc.ipynb
5. DailyBalanceCalc.py
6. plots.ipynb / plots2.ipynb


# BalanceCalc.py
calculates the balances for all addresses for every block where the address was involved in a transaction based on the token_transfer.csv

# SupplyCalc.py
based on the balances, the supply is calculated and stored in a csv file to use during the microVelocity calculation

# microVelocity.ipynb
structured in a jupiter notebook (can be changed into individual files). First calculates the holding time of all transactions and then the microVelocity of all addresses and stores it in multiple csv files

# DailyBalanceCalc.py
used to create a dataframe of daily wealth snapshots for the analysis of wealth and microVelocity

