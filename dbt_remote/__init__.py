from pathlib import Path

# In case the user never ran dbt init before.
# dbt itself will fail if this dir does not exist
(Path.home() / ".dbt").mkdir(exist_ok=True)
