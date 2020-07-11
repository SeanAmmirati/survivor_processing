from ..helpers.db_funcs import search_based_on_first_name_season_id
from ..helpers.transform_helpers import add_to_df
import pandas as pd


def map_contestants(df, contestant_season_to_id, *args, **kwargs):
    key = df['contestant'].str.lower() + '_' + df['season_id'].astype(str)
    return pd.DataFrame(key.map(contestant_season_to_id))


def transform_confessional_df(conf_df, contestant_season_to_id):

    processing_columns = {
        ('contestant_id',): map_contestants,
    }
    added = add_to_df(conf_df, processing_columns,
                      contestant_season_to_id=contestant_season_to_id,
                      inplace=False)
    drop_cols = ['contestant']

    added = added.drop(columns=drop_cols)
    return added


def create_contestant_season_dict(eng, conf_df):
    contestant_map_cols = ['contestant', 'season_id']
    u_contest_season = conf_df.drop_duplicates(subset=contestant_map_cols)[
        contestant_map_cols]
    contestant_season_to_id_df = u_contest_season.apply(lambda x:
                                                        search_based_on_first_name_season_id(eng, *x), axis=1)
    contestant_season_to_id_dict = contestant_season_to_id_df.set_index('key')[
        'id'].to_dict()
    return contestant_season_to_id_dict


def transform_confessionals(eng, conf_df):
    manual_additions = {
        'cochran_5': 123,
        'roxy_35': 421,
        'rc_35': 500,
        'dawson_35': 155,
        'bobby jon_17': 301,
        'rodger_28': 46,
        'bobby jon_25': 302,
        'kristin_25': None,
        'mindy_25': None,
        'mike_25': None,
        'michelle_40': 747,
        'shii ann_13': 15,
        'sue_13': 260,
        'cochran_34': 122,
        'jason_18': 289,
        'trish_22': None,
        'boston_22': None,
        'shii ann_12': 14,
        'matt_29': 193
    }
    contestant_season_to_id = create_contestant_season_dict(eng, conf_df)
    contestant_season_to_id.update(manual_additions)
    df = transform_confessional_df(conf_df, contestant_season_to_id)
    df['day'] = df['day'].astype(int)
    df['n_from_player'] = df['n_from_player'].astype(int)
    df['total_confessionals_in_episode'] = df['total_confessionals_in_episode'].astype(
        int)
    return df
