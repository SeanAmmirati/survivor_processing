from docx import Document
from copy import deepcopy
import requests
import os
import bs4
from openpyxl import load_workbook
import pandas as pd
from ..helpers.db_funcs import get_ep_id, get_ep_id_by_number, get_season_id_by_number_type
from ..helpers.extract_helpers import search_for_new_seasons
import glob
import re
import numpy as np

season_type_map = {
    's': 'Survivor',
    'au': 'Australian Survivor',
    'nz': 'Survivor New Zealand',
    'ssa': 'Survivor South Africa'
}


def dfize_doc(doc):
    df = pd.DataFrame()
    day = None
    order = 1
    extracted_names = ['contestant', 'n_from_player',
                       'total_confessionals_in_episode', 'content', 'day', 'n_in_episode']
    for p in doc.paragraphs:
        day_match = re.match('Day (\d+)', p.text)
        conf_match = re.match('(\w+ ?\w+) \((\d+)\/(\d+)\)\: (.*)', p.text)

        if day_match:
            day = day_match.group(1)

        if day:
            if conf_match:
                extracted = conf_match.groups(0)
                srs = pd.Series(list(extracted) +
                                [day, order], index=extracted_names)
                df = df.append(srs, ignore_index=True)
                order += 1

    return df


def collect_confessionals(raw_files, eng):
    full_df = pd.DataFrame()
    regex_file = '(\d+)x(\d+)_ (.*) (\(Pt\. \d\))?\.docx'
    for f in raw_files:
        basename = os.path.basename(f)
        m = re.match(regex_file, basename)
        if m:
            season, episode, episode_name, _ = m.groups(0)
            doc = Document(f)
            df = dfize_doc(doc)
            season_id = get_season_id_by_number_type(
                eng, int(season), 'Survivor')
            episode_id = get_ep_id(eng, episode_name, season_id)

            df['season_id'] = season_id
            df['episode_id'] = episode_id
            full_df = pd.concat([full_df, df], ignore_index=True)

    return full_df


def extract_confessionals(eng,
                          data_path='../data/raw/Survivor Confessional_s Archive (1-10, 11_, 12_, 21-35, 40)',
                          asof=None):
    raw_files = glob.glob(os.path.join(data_path, '*', '*'))
    new_seasons = search_for_new_seasons(eng, asof=asof)

    new_seasons_df = pd.read_sql(con=eng,
                                 sql='SELECT * FROM survivor.season').set_index('name').loc[new_seasons]
    search_string = new_seasons_df['season_number'].astype(
        int).astype(str).str.zfill(2) + 'x'
    print(search_string)

    raw_files = [f for f in raw_files if any(
        re.search(y, f) for y in search_string)]
    print(raw_files)

    full_df = collect_confessionals(raw_files, eng)
    return full_df
