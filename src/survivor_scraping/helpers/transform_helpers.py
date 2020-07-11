import pandas as pd


def sync_with_remote(df, eng, table_name):
    specs = pd.read_sql("""SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name='{table_name}'""".format(table_name=table_name),
                        con=eng).set_index('column_name')

    pg_dtype_to_python = {
        'double precision': float,
        'boolean': bool,
        'bigint': float,
        'text': str,
        'character': str
    }
    remote_columns = specs.index

    not_in_df = list(set(remote_columns) - set(df.columns) - {'index'})
    for col in not_in_df:
        df[col] = None

    for col in remote_columns:
        if col == 'index':
            continue
        try:
            dtype = pg_dtype_to_python[specs.loc[col, 'data_type']]
        except KeyError:
            continue
        df[col] = df[col].astype(dtype)

    # For now, just remove anything not on remote (since it has years of history...)
    not_on_remote = list(set(df.columns) - set(remote_columns))
    rem_cols_str = ",".join(not_on_remote)
    print('Removed columns: {rem_cols_str}'.format(rem_cols_str=rem_cols_str))

    df.drop(columns=not_on_remote, inplace=True)
    return df


def add_to_df(raw_df, processing_dict, inplace=False, *args, **kwargs):
    if not inplace:
        raw_df = raw_df.copy()

    for col_names, f in processing_dict.items():
        res = f(raw_df, *args, **kwargs)
        raw_df[[col for col in col_names]] = res

    if not inplace:
        return raw_df


def coerce_col(col, type):
    def ret_func(df, *args, **kwargs):
        return df[col].astype(type)
    return ret_func


def process_viewership(df, viewership_col='viewership', *args, **kwargs):
    try:
        ret_col = df[viewership_col].str.extract(
            '(\d+\.\d+)').astype(float) * 1e8
    except KeyError:
        ret_col = pd.DataFrame(np.repeat(None, df.shape[0]))
        ret_col.iloc[:, 0] = ret_col.iloc[:, 0].astype(float)
    return ret_col
