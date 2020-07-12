from ..helpers.db_funcs import create_season_times, create_episode_times
from ..helpers.transform_helpers import add_to_df, coerce_col, sync_with_remote

import pandas as pd
from collections import OrderedDict


def process_utc(df, *args, **kwargs):
    return pd.DataFrame(pd.to_datetime(df['created_utc'], unit='s'))


def process_nearest_event(df, event_times, match_time_col,
                          event_time_col, subset_cols=None,
                          direction='forward'):
    event_times = event_times[event_times[event_time_col].notnull()]
    sorted_times = event_times.sort_values(by=event_time_col)
    merged_asof = pd.merge_asof(df[df[match_time_col].notnull()],
                                sorted_times,
                                left_on=[match_time_col],
                                right_on=[event_time_col])

    if not subset_cols:
        subset_cols = merged_asof.columns

    return merged_asof[subset_cols]


def process_within_season(df, season_times, *args, **kwargs):
    merged = df.merge(season_times, left_on='most_recent_season',
                      right_on='season_id')

    within = merged['most_recent_season']
    within[merged['created_dt'] > merged['showing_ended']] = None
    return pd.DataFrame(within)


def process_nearest_season_started(df, season_times, *args, **kwargs):
    return process_nearest_event(df, season_times, 'created_dt',
                                 'showing_started', ['season_id'])


def process_nearest_episode_aired(df, episode_times, *args, **kwargs):
    return process_nearest_event(df, episode_times, 'created_dt',
                                 'firstbroadcast', ['episode_id'])

# def stringize_dict_and_list_cols(df):


def transform_reddit(reddit_dfs, eng):
    season_times = create_season_times(eng)
    episode_times = create_episode_times(eng)
    ret_list = []

    table_names = ['submissions', 'comments']
    table_names = ['reddit_' + t for t in table_names]

    for i, df in enumerate(reddit_dfs):
        if df.empty:
            ret_list.append(df)
        df[['created_dt']] = process_utc(df)

        processing_columns = OrderedDict()

        processing_columns[('created_dt',)] = process_utc
        processing_columns[('most_recent_season', )
                           ] = process_nearest_season_started
        processing_columns[('most_recent_episode',)
                           ] = process_nearest_episode_aired
        processing_columns[('within_season', )] = process_within_season

        added = add_to_df(df, processing_columns,
                          inplace=False, season_times=season_times,
                          episode_times=episode_times)

        dict_cols = added.columns[added.applymap(
            lambda x: isinstance(x, dict) or isinstance(x, list)).any()]

        for col in dict_cols:
            added.loc[df[col].notnull(), col] = \
                added.loc[df[col].notnull(), col].astype(str)

        added = sync_with_remote(added, eng, table_names[i])

        ret_list.append(added)

    # TODO: IT would be cool to transform the flairs, or mentions, to the contestant IDs

    return ret_list
