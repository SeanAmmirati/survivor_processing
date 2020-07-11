import bs4
import pandas as pd
import requests
import re

from ..helpers.db_funcs import get_season_id
from ..helpers.extract_helpers import check_season_status, search_for_new_seasons


def season_url(locality, season):

    if season != '':
        suffix = locality.replace(' ', '_') + ':_' + season.replace(' ', '_')
    else:
        suffix = locality.replace(' ', '_')

    return 'https://survivor.fandom.com/wiki/' + suffix


def process_season(locality, season):
    url = season_url(locality, season)
    soup = bs4.BeautifulSoup(requests.get(url).content, features="lxml")
    srs = extract_season_info(soup)
    return srs


def extract_season_info(soup):
    results = {}

    results['summary'] = ''
    results['history'] = ''
    results['trivia'] = ''
    results['twists'] = ''

    keys = ['version', 'season',
            'location', 'filmingdates',
            'seasonrun', 'episodes', 'days', 'survivors',
            'winner', 'runnerup', 'viewership']
    for k in keys:
        srch = soup.find_all(['div', 'figure'], {'data-source': k})

        if srch:
            div = srch[0]

            if 'image' in k:
                ret = [l for x, l in div.find_all(
                    'img')[0].attrs.items() if x == 'src'][0]
            elif k != 'runnerup':
                ret = div.text.split('\n')[-2]

            else:
                ret = [y.text for y in div.find_all('a')]

            results[k] = ret

    for i, p in enumerate(soup.find_all('p')):
        if 'This section is empty' in p.text:
            continue
        if p.find_all_next('span', {'id': 'Production'}):

            # We are in the summary section
            results['summary'] += p.text
        elif (not p.find_all_next('span', {'id': 'Season_Summary'})) and (p.find_all_next('span', {'id': 'Voting_History'})):
            # we are in the members section, skip this
            pass
        elif p.find_all_next('span', {'id': 'Trivia'}):
            # we are in the tribal history
            results['history'] += p.text

    for b in soup.find_all('li'):
        if not(b.find_all_next('span', {'id': 'Production'})) and (b.find_all_next('span', {'id': 'Castaways'})):
            # twists stage
            results['twists'] += '* ' + b.text
        if not(b.find_all_next('span', {'id': 'Trivia'})) and (b.find_all_next('span', {'id': 'References'})):
            # twists stage
            results['trivia'] += '* ' + b.text

    return pd.Series(results)


def determine_season_index(con):
    q = 'SELECT MAX(season_id) + 1 FROM survivor.season'
    index = con.execute(q).fetchall()[0][0]
    return index


def extract_seasons(con, asof=None):
    new_seasons = search_for_new_seasons(con, asof)

    srs_list = []
    if new_seasons:
        new_idx = determine_season_index(con)
        locality = 'Survivor'
        for season in new_seasons:
            data = process_season(locality, season)
            data['name'] = season
            data['type'] = 'Survivor'

            if check_season_status(con, season, asof) == 1:
                idx = get_season_id(con, season)
            else:
                idx = new_idx
                new_idx += 1

            data['season_id'] = idx

            srs_list.append(data)
    return pd.DataFrame(srs_list)
