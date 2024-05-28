import pandas as pd

_VALID_ENGINE_TYPE = {
    'csv',
    'excel',
    'parquet'
}

_ENGINE = {
    'csv': None,
    'excel': 'openpyxl',
    'parquet': 'pyarrow'
}

_READER = {
    'csv': pd.read_csv,
    'excel': pd.read_excel,
    'parquet': pd.read_parquet
}

def _get_read_func(file_type):
    return _READER[file_type]

def _get_engine(file_type):
    return _ENGINE[file_type]

def _get_save_func(df, file_type):
    _SAVER = {
        'csv': df.to_csv,
        'excel': df.to_excel,
        'parquet': df.to_parquet
    }
    return _SAVER[file_type]