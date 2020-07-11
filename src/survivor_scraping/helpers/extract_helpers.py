import requests
import bs4
import pandas as pd
from datetime import datetime


def search_for_new_seasons(con, asof=None):
    r = requests.get('https://survivor.fandom.com/wiki/Category:Seasons')
    sp = bs4.BeautifulSoup(r.content, features="lxml")

    not_found = []
    for li in sp.find_all(name='li', attrs={'class': 'category-page__member'}):
        title = li.find_next('a').attrs['title']
        if 'Survivor:' in title:
            short = title.replace('Survivor: ', '')
            short = edit_short(short)
            if check_season_status(con, short, asof) > 0:

                not_found.append(short)

    return not_found


def check_season_status(con, season_name, asof=None):
    """
    Three possible results:

    0: This is a past season
    1: This is a currently running season
    2: This is a future season

    """
    q = "SELECT name, showing_started, showing_ended FROM survivor.season WHERE name = '{season_name}'".format(
        season_name=season_name)

    df = pd.read_sql(q, con=con)

    if df.empty:
        return 2

    else:
        if not asof:
            now = datetime.now()
        else:
            now = pd.to_datetime(asof)

        no_end_date_showing_but_start_showing = (
            df['showing_started'].notnull() & df['showing_ended'].isnull())
        end_showing_after_today = df['showing_ended'] > now
        currently_running_seasons = df.loc[no_end_date_showing_but_start_showing |
                                           end_showing_after_today, 'name'].tolist()

        if currently_running_seasons:
            return 1
        else:

            return 0


def edit_short(short):
    short = 'Heroes vs. Villains' if short == 'Heroes vs Villains' else short
    short = short.replace('Kaoh Rong', 'Kaôh Rōng')
    return short
