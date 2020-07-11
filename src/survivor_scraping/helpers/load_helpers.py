import pandas as pd


def upsert(df, eng, table, key_cols):
    if df.empty:
        return
    remote_cols = pd.read_sql(
        f'SELECT * FROM survivor.{table} LIMIT 1', coerce_float=True, con=eng)
    remote_cols = [c for c in remote_cols if c not in ['updated', 'created']]

    conf_str = ', '.join(key_cols)

    select_lines = ', '.join(remote_cols)

    set_lines = ''

    for col in df:
        set_lines += f'{col} = excluded.{col}, '

    df.to_sql(f'temp_table_{table}', con=eng,
              schema='survivor', if_exists='replace')

    set_lines = set_lines[:-2]

    q = f'''INSERT INTO survivor.{table}
           SELECT {select_lines}
           FROM survivor.temp_table_{table}
           ON CONFLICT ({conf_str})
           DO
           UPDATE SET ''' + set_lines

    eng.execute(q)

    q2 = f'''DROP TABLE survivor.temp_table_{table}'''

    eng.execute(q2)
