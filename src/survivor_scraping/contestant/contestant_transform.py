from copy import deepcopy
import pandas as pd
from ..helpers.transform_helpers import add_to_df, process_viewership, sync_with_remote
from ..helpers.db_funcs import (create_full_name_season_srs, create_season_name_to_id,
                                get_attempt_number, pull_agg_contestant_stats,
                                get_tribe_id, get_alliance_id)
import re


def grab_name(string):
    m = re.match('(\w* \w*)', string)
    if m:
        return m.group(1)


def process_attempt_numbers(df, con, *args, **kwargs):
    attempt_number = df[['contestant_season_id', 'contestant_id']].apply(
        lambda x: get_attempt_number(con, x[0], x[1]), axis=1)
    return pd.DataFrame(attempt_number)


def find_tribe_by_attempt_number(row, con):
    attempt_num_int = int(row["attempt_number"])
    attempt_num_int = '' if attempt_num_int == 1 else attempt_num_int
    column_name = f'tribes{attempt_num_int}'
    season_id = row['season_id']
    tribe_ids = [None] * 4

    for i, tribe in enumerate(row[column_name]):
        tribe_cleaned = tribe.replace('► ', '')
        tribe_id = get_tribe_id(con, tribe_cleaned, season_id)
        tribe_ids[i] = tribe_id

    return pd.Series(tribe_ids)


def find_alliance_by_attempt_number(row, con):
    attempt_num_int = int(row["attempt_number"])
    attempt_num_int = '' if attempt_num_int == 1 else attempt_num_int
    column_name = f'alliances{attempt_num_int}'
    season_id = row['season_id']
    alliance_ids = [None] * 3
    if column_name not in row.index:
        print(row.index)
        return pd.Series(alliance_ids)
    print(row[column_name])
    if isinstance(row[column_name], list):

        for i, alliance in enumerate(row[column_name]):
            alliance_cleaned = alliance.replace('► ', '')
            alliance_id = get_alliance_id(con, alliance_cleaned, season_id)
            print(alliance_id)
            alliance_ids[i] = alliance_id
    return pd.Series(alliance_ids)


def process_tribe_contestant(df, con, *args, **kwargs):
    tribe_ids = df.apply(
        lambda x: find_tribe_by_attempt_number(x, con), axis=1)
    return tribe_ids


def process_alliance_contestant(df, con, *args, **kwargs):
    alliance_ids = df.apply(
        lambda x: find_alliance_by_attempt_number(x, con), axis=1)
    return alliance_ids


def process_name_func(col_name, full_name_to_id, size=1):

    def process_name_column(df, *args, **kwargs):
        keys_expanded = df[col_name].apply(lambda x: pd.Series(
            [grab_name(n) for n in str(x).split('& ')]))
        keys_expanded = keys_expanded.apply(
            lambda x: x.astype(str) + ' ' + df['season_id'].fillna(0).astype(int).astype(str))
        keys_expanded = keys_expanded.apply(lambda x: x.map(full_name_to_id))
        while keys_expanded.shape[1] < size:
            key = keys_expanded.shape[1]
            keys_expanded[key] = None
            keys_expanded[key] = keys_expanded[key].astype(float)

        return keys_expanded
    return process_name_column


# def convert_grouping(df, name='tribe', size=3):

#     def convert_group(df):
#         df[name]


def process_opponents(df, size=3, *args, **kwargs):
    opponents = pd.DataFrame()
    expanded = df['opponents'].apply(pd.Series)

    for col in expanded:
        opponents[col] = expanded.merge(
            df, left_on=col, right_on='name', how='left')['tribe_id']

    while opponents.shape[1] < size:
        opponents[opponents.shape[1]] = None
    return opponents


def transform_alliance_df(alliance_df, full_name_to_id):

    processing_columns = {
        ('lowest_placing_member',): process_name_func('lowestplacingmember', full_name_to_id),
        ('highest_placing_0', 'highest_placing_1'): process_name_func('highestplacingmember', full_name_to_id, size=2),
        ('founder_0', 'founder_1', 'founder_2'): process_name_func('founder', full_name_to_id, size=3),

    }
    added = add_to_df(alliance_df, processing_columns,
                      inplace=False)

    drop_cols = ['founder', 'lowestplacingmember',
                 'highestplacingmember', 'season']

    added = added.drop(columns=drop_cols)

    return added


def transform_tribe_df(tribe_df, full_name_to_id):

    processing_columns = {
        ('lowest_placing_member',): process_name_func('lowestplacingmember', full_name_to_id),
        ('highest_placing_member', ): process_name_func('highestplacingmember', full_name_to_id, size=1),
        ('opponent_0', 'opponent_1', 'opponent_2'): process_opponents
    }
    added = add_to_df(tribe_df, processing_columns,
                      inplace=False)
    drop_cols = ['lowestplacingmember', 'opponents',
                 'highestplacingmember', 'season']

    added = added.drop(columns=drop_cols)
    return added


def transform_contestant_df(contestant, con):

    contestant_level_cols = ['wiki_survivor_text',
                             'wiki_postsurvivor_text',
                             'trivia',
                             'birthdate',
                             'other_profile',
                             'hometown',
                             'current_residence',
                             'occupation_self_reported',
                             'hobbies',
                             'pet_peeves',
                             'three_words',
                             'claim_to_fame',
                             'inspiration',
                             'three_things',
                             'most_similar_self_reported',
                             'reason',
                             'why_survive',
                             'previous_season',
                             'first_name', 'last_name', 'nickname', 'twitter', 'sex', 'image', 'wikia']

    relevant_columns = contestant.columns[contestant.columns.isin(
        contestant_level_cols)]
    contestant_season = contestant.drop(columns=relevant_columns).copy()
    contestant = contestant[relevant_columns.tolist(
    ) + ['contestant_id']].drop_duplicates()

    processing_columns = {
        ('attempt_number',): process_attempt_numbers,
        ('tribe_0', 'tribe_1', 'tribe_2', 'tribe_3'): process_tribe_contestant,
        ('alliance_0', 'alliance_1', 'alliance_2'): process_alliance_contestant
    }
    added_contestant_season = add_to_df(contestant_season, processing_columns, con=con,
                                        inplace=False)

    agg_q_results = pull_agg_contestant_stats(con)
    added_contestant_season = added_contestant_season.merge(
        agg_q_results, on='contestant_season_id')

    contestant_season_drops = added_contestant_season.columns[
        added_contestant_season.columns.str.contains('tribes|alliances')
    ].tolist()

    contestant_season_drops += ['first_name', 'last_name', 'season_id_x']

    added_contestant_season = added_contestant_season.drop(
        columns=contestant_season_drops)
    added_contestant_season.rename(
        columns={'season_id_y': 'season_id'}, inplace=True)

    added_contestant_season = sync_with_remote(
        added_contestant_season, con, 'contestant_season')
    contestant = sync_with_remote(contestant, con, 'contestant')

    return added_contestant_season, contestant


def transform_contestants(df_list, eng):
    name_mapping = create_full_name_season_srs(eng)
    name_mapping = name_mapping.iloc[:, 0].to_dict()
    tribal = transform_tribe_df(df_list[1], name_mapping)
    alliance = transform_alliance_df(df_list[2], name_mapping)
    contestant_season, contestant = transform_contestant_df(
        df_list[0], con=eng)

    return contestant_season, contestant, alliance, tribal
