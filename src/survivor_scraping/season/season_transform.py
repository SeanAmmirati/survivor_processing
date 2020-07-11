import re
import pandas as pd

import numpy as np
from ..helpers.transform_helpers import add_to_df, coerce_col, process_viewership
from ..helpers.db_funcs import create_full_name_season_srs
# Transform


def process_contestant_by_name(df, name_mapping, name_col, new_col=None):
    if not new_col:
        new_col = name_col

    expanded = df[name_col].apply(pd.Series)
    expanded.columns = new_col + '_id_' + expanded.columns.astype(str)
    keys = expanded.apply(lambda x: x + ' ' + df['season_id'].astype(str))

    cs_ids = keys.applymap(
        lambda x: name_mapping.loc[x, 'contestant_season_id'])
    return cs_ids


def process_runnerups(df, name_mapping, *args, **kwargs):
    return process_contestant_by_name(df, name_mapping, 'runnerup')


def process_winners(df, name_mapping, *args, **kwargs):
    return process_contestant_by_name(df, name_mapping, 'winner')


def process_survivorwiki_dateranges(df, dr_col):

    pattern = '(\w* \w*,? ?\w*)(\[\d\])? (-|–) (\w* \w*,? ?\w*)(\[\d\])?'
    extracted = df[dr_col].str.extract(pattern)
    dates_expanded = extracted.drop([1, 2, 4], axis=1).apply(pd.to_datetime)
    dates_expanded.columns = 'start', 'end'
    return dates_expanded


def process_film_dates(df, *args, **kwargs):

    return process_survivorwiki_dateranges(df, 'filmingdates')


def process_showing_dates(df, *args, **kwargs):
    return process_survivorwiki_dateranges(df, 'seasonrun')


def transform_season_df(season_df, name_mapping):

    processing_functions = {
        ('runnerup_0_id', 'runnerup_1_id'): process_runnerups,
        ('filming_started', 'filming_ended'): process_film_dates,
        ('winner_id',): process_winners,
        ('showing_started', 'showing_ended'): process_showing_dates,
        ('viewership_in_millions',): process_viewership,
        ('viewership',): process_viewership,
        ('days',): coerce_col('days', float),
        ('episodes',): coerce_col('episodes', float),
        ('season',): coerce_col('season', int)
    }
    added = add_to_df(season_df, processing_functions,
                      inplace=False, name_mapping=name_mapping)

    drop_columns = ['filmingdates', 'seasonrun', 'winner', 'runnerup']

    added.drop(columns=drop_columns, inplace=True)

    rename_columns = {'episodes': 'n_episodes',
                      'season': 'season_number', 'survivors': 'n_survivors'}

    added.rename(columns=rename_columns, inplace=True)

    return added


def transform_seasons(season_df, eng):
    name_srs = create_full_name_season_srs(eng)
    return transform_season_df(season_df, name_srs)
