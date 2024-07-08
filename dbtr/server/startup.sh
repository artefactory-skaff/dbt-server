if [ "$ADAPTER" = "dbt-bigquery" ]; then
    pip install $ADAPTER 'elementary-data[bigquery]'
elif [ "$ADAPTER" = "dbt-snowflake" ]; then
    pip install $ADAPTER 'elementary-data[snowflake]'
elif [ "$ADAPTER" = "dbt-redshift" ]; then
    pip install $ADAPTER 'elementary-data[redshift]'
elif [ "$ADAPTER" = "dbt-databricks" ]; then
    pip install $ADAPTER 'elementary-data[databricks]'
elif [ "$ADAPTER" = "dbt-athena" ]; then
    pip install $ADAPTER 'elementary-data[athena]'
elif [ "$ADAPTER" = "dbt-trino" ]; then
    pip install $ADAPTER 'elementary-data[trino]'
fi
python3 dbtr/server/main.py
