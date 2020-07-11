from ..helpers.load_helpers import upsert


def load_contestants(transformed_dfs, eng):
    table_names = ['contestant_season', 'contestant',
                   'alliance', 'tribe']
    conflict_cols = ['contestant_season_id',
                     'contestant_id',
                     'alliance_id',
                     'tribe_id']

    for i, tbl in enumerate(table_names):

        df = transformed_dfs[i]
        conf_list = conflict_cols[i].split(', ')
        upsert(df, eng, tbl, conf_list)
