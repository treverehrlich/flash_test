import pandas as pd
import numpy as np
from pandas.tseries.holiday import USFederalHolidayCalendar
from datetime import date, timedelta

def ensure_datetime_col(df: pd.DataFrame, col: str='date') -> pd.DataFrame:
    """
    Ensure a column is datetime typed.

    :param df: Dataframe
    :type df: pd.DataFrame
    :param col: column to check
    :type col: str
    :return: pd.DataFrame

    Example usage:

        .. code-block:: python

            df = ensure_datetime_col(df, 'dt')
            
            # If column is 'date', then simply:
            df = ensure_datetime_col(df)
    """
    if not np.issubdtype(df[col].dtype, np.datetime64):
        df[col] = pd.to_datetime(df[col])
    return df


def get_holidays(start_dt: str="2019-01-01", end_dt=date.today(), date_col_name=None):
    """
    Create a column of holiday booleans.

    :param start_dt: start date
    :type start_dt: str
    :param end_dt: end date
    :type end_dt: str or datetime
    :return: pd.DataFrame

    Example usage:

        .. code-block:: python

            holidays = get_holidays(date_col_name='my_dt')
            df = pd.merge(df, holidays, how='left', on='my_dt')
            num_holidays = df['holiday'].sum()

            # or
            holidays = get_holidays()
            df = pd.merge(df, holidays, how='left', on='date')
            num_holidays = df['holiday'].sum()
    """
    cal = USFederalHolidayCalendar()
    holi_list = (cal.holidays(start=start_dt, end=end_dt)
                .to_pydatetime()
                .tolist())

    us_holiday = {"date": holi_list}
    us_holiday = pd.DataFrame(us_holiday)
    us_holiday["date"] = pd.to_datetime(us_holiday["date"])
    us_holiday['holiday'] = True
    full_dates = pd.DataFrame({'date': pd.date_range(start_dt, end_dt)})
    full_dates = pd.merge(full_dates, us_holiday, on='date', how='left').fillna(False)
    if date_col_name:
        full_dates = full_dates.rename(columns={'date': date_col_name})
    return full_dates


def add_date_features(df: pd.DataFrame, date_col: str='date',
        numeric_date: bool=True, holidays: bool=True,
        holiday_start_dt: str="2019-01-01",
        holiday_end_dt=date.today() + timedelta(14)) -> pd.DataFrame:
    """
    Add date features to dataframe. Useful for feature engineering. The following columns will be generated:
    ['day', 'dow', 'MonthFirstDay', 'FirstWeekday', 
    'WeekInMonth', 'month', 'quarter', 'year', 'weeknum', 
    'as_mo_end', 'as_mo_start', 'as_quarter', 'as_quarter_end', 
    'as_quarter_start', 'as_year_end', 'as_year_start', 'season'] \\
    + ['holiday', 'numeric_date'] # optional

    :param df: dataframe
    :type df: pd.DataFrame
    :param date_col: name of existing date column
    :type date_col: str
    :param numeric_date: include a .toordinal() based column
    :type numeric_date: bool
    :param holidays: include a .toordinal() based column
    :type holidays: bool
    :param holiday_start_dt: holiday starting date
    :type holiday_start_dt: str
    :param holiday_end_dt: holiday ending date
    :type holiday_end_dt: str or datetime
    :return: pd.DataFrame

    Example usage:

        .. code-block:: python

            feature_df = add_date_features(df)

            feature_df = add_date_features(df, numeric_date=False, holidays=False)
    """
    df = ensure_datetime_col(df, date_col)

    df['day'] = df[date_col].dt.day
    df['dow'] = df[date_col].dt.dayofweek
    df['MonthFirstDay'] = df[date_col] - pd.to_timedelta(df[date_col].dt.day - 1, unit='d')
    df['FirstWeekday'] = df['MonthFirstDay'].dt.weekday
    df['WeekInMonth'] = (df[date_col].dt.day - 1 + df['FirstWeekday']) // 7 + 1
    # df = df.drop(columns=['MonthFirstDay', 'FirstWeekday'])  
    df['month'] = df[date_col].dt.month
    df['quarter'] = df[date_col].dt.quarter
    df['year'] = df[date_col].dt.year
    df['weeknum'] = df[date_col].dt.week
    df['as_mo_end'] = df[date_col].dt.is_month_end.astype(int)
    df['as_mo_start'] = df[date_col].dt.is_month_start.astype(int)
    df['as_quarter'] = df[date_col].dt.quarter
    df['as_quarter_end'] = df[date_col].dt.is_quarter_end.astype(int)
    df['as_quarter_start'] = df[date_col].dt.is_quarter_start.astype(int)
    df['as_year_end'] = df[date_col].dt.is_year_end.astype(int)
    df['as_year_start'] = df[date_col].dt.is_year_start.astype(int)
    df["season"] = np.where((df["month"] > 2) & (df["month"] <= 5), 2,
                                (np.where((df["month"] > 5) & (df["month"] <= 8), 3,
                                        (np.where((df["month"] > 8) & (df["month"] <= 11), 4, 1, )), )), )
    if numeric_date:
        df['numeric_date'] = df[date_col].transform(lambda date: date.toordinal())

    if holidays:
        holidays = get_holidays(holiday_start_dt, holiday_end_dt, date_col)
        df = pd.merge(df, holidays, how='left', on=date_col)
    return df