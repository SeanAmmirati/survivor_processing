import pandas as pd


def upsert(df, eng, table, key_cols):
    if df.empty:
        return
    remote_cols = pd.read_sql(
        'SELECT * FROM survivor.{table} LIMIT 1'.format(table=table),
        coerce_float=True, con=eng)
    remote_cols = [c for c in remote_cols if c not in ['updated', 'created']]

    conf_str = ', '.join(key_cols)

    select_lines = ', '.join(remote_cols)

    set_lines = ''

    for col in df:
        set_lines += '{col} = excluded.{col}, '.format(col=col)

    df.to_sql('temp_table_{table}'.format(table=table), con=eng,
              schema='survivor', if_exists='replace')

    set_lines = set_lines[:-2]

    fmt_dict = dict(table=table, select_lines=select_lines, conf_str=conf_str)

    q = '''INSERT INTO survivor.{table}
           SELECT {select_lines}
           FROM survivor.temp_table_{table}
           ON CONFLICT ({conf_str})
           DO
           UPDATE SET '''.format(**fmt_dict) + set_lines

    eng.execute(q)

    q2 = '''DROP TABLE survivor.temp_table_{table}'''.format(table=table)

    eng.execute(q2)
