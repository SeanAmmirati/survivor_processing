from copy import deepcopy
from ..helpers.db_funcs import create_full_name_season_srs
import yaml
from ..helpers.transform_helpers import sync_with_remote
import os


def ic_transform(df, full_name_dict_to_id):

    iterative_replace_null(df, 'win', ['win?', 'win? ', 1])
    iterative_replace_null(df, 'sitout', ['sitout', 'SO'])

    merge_key = df['contestant'].str.replace(
        ' ', '_') + '_' + df['season_id'].astype(str)
    df['contestant_id'] = merge_key.map(full_name_dict_to_id)

    rel_columns = df.columns[df.columns.isin(
        ['win?', 'contestant', 'SO', 'win? ', 'episode'])]
    df.rename(columns={'#people': 'team', 'win%': 'win_pct',
                       'total win%': 'episode_win_pct'}, inplace=True)
    df.drop(columns=rel_columns, inplace=True)

    df = df[df['win'].notnull()].reset_index(drop=True)

    df['tc_number'] = df['tc_number'].astype(float)
    df['total_players_remaining'] = df['total_players_remaining'].astype(float)
    df['tc_number'] = df['tc_number'].fillna(0)

    return df


def iterative_replace_null(df, new_col, l_cols):
    if new_col not in df:
        df[new_col] = None
    for col in l_cols:
        if col in df:
            df[new_col] = df[new_col].fillna(df[col])


def rc_transform(df, full_name_dict_to_id):

    iterative_replace_null(df, 'win', ['win?', 'win? ', 1])
    iterative_replace_null(df, 'win_pct', ['win%', .25])
    iterative_replace_null(df, 'team', ['#people', 4])
    iterative_replace_null(df, 'episode_win_pct', ['total win%', 1.25])

    merge_key = df['contestant'].str.replace(
        ' ', '_') + '_' + df['season_id'].astype(str)
    df['contestant_id'] = merge_key.map(full_name_dict_to_id)
    rel_columns = df.columns[df.columns.isin(
        ['Abi-Maria', 'contestant', 'win?', '#people', 'win%', 'total win%', 1, 0.25, 1.25, 4, 'win? ', 'episode'])]

    df.drop(columns=rel_columns, inplace=True)
    df = df[df['win'].notnull()].reset_index(drop=True)

    df['tc_number'] = df['tc_number'].astype(float)
    df['total_players_remaining'] = df['total_players_remaining'].astype(float)

    df['challenge_number'] = df['challenge_number'].fillna(1)
    df['tc_number'] = df['tc_number'].fillna(0)
    return df


def tc_transform(df, full_name_dict_to_id):
    merge_key_c = df['contestant'].str.replace(
        ' ', '_') + '_' + df['season_id'].astype(str)
    df['contestant_id'] = merge_key_c.map(full_name_dict_to_id)

    merge_key_v = df['voted_for'].str.replace(
        ' ', '_') + '_' + df['season_id'].astype(str)
    df['voted_for_id'] = merge_key_v.map(full_name_dict_to_id)

    rel_columns = df.columns[df.columns.isin(
        ['contestant', 'voted_for', 'vote_counted', 'episode'])]

    df = df[df['voted_for'].notnull()].reset_index(drop=True)

    should_be_unique = ['season_id', 'episode_id',
                        'tc_number', 'contestant_id']

    df['vote_number'] = df.groupby(should_be_unique)[
        'total_players_remaining'].apply(lambda x: x.rank(method='first'))

    df.drop(columns=rel_columns, inplace=True)

    return df


def overall_transform(df, full_name_dict_to_id):
    df_names = {
        'ChW': 'challenge_wins',
        'ChA': 'challenge_appearances',
        'SO': 'sitouts',
        'VFB': 'voted_for_bootee',
        'VAP': 'votes_against_player',
        'TotV': 'total_number_of_votes_in_episode',
        'TCA': 'tribal_council_appearances',
        'JVF': 'number_of_jury_votes',
        'TotJ': 'total_number_of_jury_votes',
        'VFT': 'votes_at_council', ""
        'tot days': 'number_of_days_spent_in_episode',
        'exile days': 'days_in_exile',
        'InRCA': 'individual_reward_challenge_appearances',
        'InRCW': 'individual_reward_challenge_wins',
        'InICW': 'individual_immunity_challenge_wins',
        'InICA': 'individual_immunity_challenge_appearances',
        'TRCA': 'tribal_reward_challenge_appearances',
        'TRCW': 'tribal_reward_challenge_wins',
        'TICA': 'tribal_immunity_challenge_appearances',
        'TICW': 'tribal_immunity_challenge_wins',
        'TRC 2nd': 'tribal_reward_challenge_second_of_three_place',
        'TIC 2nd': 'tribal_immunity_challenge_second_of_three_place',
        'FT ch': 'fire_immunity_challenge',
        'TIC 3rd': 'tribal_immunity_challenge_third_place',
    }

    merge_key = df['contestant'].str.replace(
        ' ', '_') + '_' + df['season_id'].astype(str)
    df['contestant_id'] = merge_key.map(full_name_dict_to_id)

    df = df.rename(columns=df_names)
    df = df[df['episode_id'].notnull() & df['challenge_wins'].notnull()]
    rel_columns = df.columns[df.columns.isin(
        ['totJ', 'contestant', 'episode', 'season_type', 'season_number', 'Unnamed: 0'])]

    if 'totJ' in df:
        df['total_number_of_jury_votes'] = df['total_number_of_jury_votes'].fillna(
            df['totJ'])
    df.drop(columns=rel_columns, inplace=True)

    keys = ['contestant_id', 'season_id', 'episode_id']

    df = df.groupby(
        keys)[df.columns[~df.columns.isin(keys)]].sum().reset_index()

    return df


def transform_episodal_data(dfs, full_name_dict_to_id, *args, **kwargs):
    dfs = deepcopy(dfs)
    dfs['overall_episode'] = overall_transform(
        dfs['overall_episode'].copy(), full_name_dict_to_id)
    dfs['immunity_challenge'] = ic_transform(
        dfs['immunity_challenge'].copy(), full_name_dict_to_id)
    dfs['reward_challenge'] = rc_transform(
        dfs['reward_challenge'].copy(), full_name_dict_to_id)
    dfs['tribal_council'] = tc_transform(
        dfs['tribal_council'].copy(), full_name_dict_to_id)
    return dfs


def transform_episode_stats_w_dict(dfs, full_name_dict_to_id, *args, **kwargs):
    dfs = deepcopy(dfs)
    if not dfs['overall_episode'].empty:
        dfs['overall_episode'] = overall_transform(
            dfs['overall_episode'].copy(), full_name_dict_to_id)

    if not dfs['immunity_challenge'].empty:
        dfs['immunity_challenge'] = ic_transform(
            dfs['immunity_challenge'].copy(), full_name_dict_to_id)

    if not dfs['reward_challenge'].empty:
        dfs['reward_challenge'] = rc_transform(
            dfs['reward_challenge'].copy(), full_name_dict_to_id)

    if not dfs['tribal_council'].empty:
        dfs['tribal_council'] = tc_transform(
            dfs['tribal_council'].copy(), full_name_dict_to_id)
    return dfs


def transform_episode_stats(dfs, eng):
    full_name = create_full_name_season_srs(eng).iloc[:, 0].to_dict()

    data_dir = os.path.join(os.path.dirname(__file__),
                            '../../../data/interim')
    truedorks_yaml_loc = os.path.join(
        data_dir, 'truedorks_contestant_namemap.yaml')
    with open(truedorks_yaml_loc, 'r') as f:
        full_name.update(yaml.load(f))

    dfs = transform_episode_stats_w_dict(dfs, full_name)
    dfs['overall_episode'] = sync_with_remote(
        dfs['overall_episode'], eng, 'episode_performance_stats')
    return dfs
