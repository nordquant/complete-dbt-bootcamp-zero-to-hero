import holidays

def is_holiday(date_col):
    german_holidays = holidays.Germany()
    return date_col in german_holidays

def model(dbt, _session):
    dbt.config(
        materialized = "table",
        packages = ["holidays"]
    )

    # df means data frame
    orders_df = dbt.ref("seed_full_moon_dates")
    # pandas is the native toolkit for dataframe.
    df = orders_df.to_pandas()
    df["IS_HOLIDAY"] = df["FULL_MOON_DATE"].apply(is_holiday)
    return df
