import os
import re
import requests

import bs4

import pandas as pd

from season_extract import season_url
from ..helpers.db_funcs import get_ep_id, get_season_id

from ..helpers.extract_helpers import search_for_new_seasons


def parse_quote_table(quote_table):
    tbl = quote_table

    speaker_attrs = tbl.find_all('a')
    if not speaker_attrs:
        raise ValueError

    speaker_selected = None
    found = False
    for speaker_attr in speaker_attrs:
        imgs = speaker_attr.find_all('img')
#         if (speaker_attrs.find_next('img').find_next('span', {'id': 'Still_in_the_Running'})):
        if imgs:
            for img in imgs:
                if img.find_next('span', {'id': 'Still_in_the_Running'}):
                    if 'title' in speaker_attr.attrs:
                        speaker_selected = speaker_attr
                        found = True
                        break
        if found:
            break
        speaker_selected = speaker_attr
    try:
        speaker = speaker_selected.attrs['title']
    except KeyError:
        raise

    if speaker.strip() == '':
        speaker = 'General'

    spans = tbl.find_all('span')
    found = False
    for span in spans:

        words = span.text
        if words in ['No revote confessionals were shown in the episode.\n', 'Voting Confessionals']:
            continue
        if (words.strip() in ['', '"', '“', '”']) or re.match('^–', words.strip()):
            continue
        if span.parent.name == 'a':
            continue
        else:
            found = True
            break

    if not found:
        raise ValueError('Could not find valid quote')

    return speaker, words


def extract_episode_urls_from_season_soup(soup):
    episodes = []

    season_summary = soup.find_all('span', attrs={'id': 'Season_Summary'})[0]

    cursor = season_summary

    while cursor.find_all_next('span', attrs={'id': 'Voting_History'}):
        cursor = cursor.find_next('td')

        # Number column
        if re.match('^ ?\d+$', cursor.text):
            episode_suffix = cursor.find_next(
                'td').find_next('a').attrs['href']
            episode_url = os.path.join(
                'https://survivor.fandom.com', episode_suffix[1:])
            episodes.append(episode_url)

    return episodes


def extract_episode_info(soup):
    results = {}

    results['summary'] = ''
    results['story'] = ''
    results['challenges'] = ''
    results['trivia'] = ''
    results['voting_confessionals'] = {}
    results['final_words'] = {}
    results['story_quotes'] = {}

    keys = ['image', 'episodenumber', 'firstbroadcast', 'viewership',
            'share']

    any_final_words = soup.find_all('span', {'id': 'Final_Words'})
    for k in keys:
        srch = soup.find_all(['div', 'figure'], {'data-source': k})

        if srch:
            div = srch[0]

            if 'image' in k:
                ret = [l for x, l in div.find_all(
                    'img')[0].attrs.items() if x == 'src'][0]

            else:
                ret = div.find_all('div', {'class': {'pi-data-value'}})[0].text

            results[k] = ret

    # final_words

    for i, p in enumerate(soup.find_all('p')):
        if 'This section is empty' in p.text:
            continue
        if p.find_all_next('span', {'id': 'Story'}):

            # We are in the summary section
            results['summary'] += p.text
        elif p.find_all_next('span', {'id': 'Challenges'}):
            # we are in the members section, skip this
            results['story'] += p.text
        elif p.find_all_next('span', {'id': 'Tribal_Council'}):
            # we are in the tribal history
            results['challenges'] += p.text

    for tbl in soup.find_all('table', {'class': 'cquote'}):
        if tbl.find_all_next('span', {'id': 'Voting_Confessionals'}):

            try:
                speaker, words = parse_quote_table(tbl)
            except (KeyError, ValueError, IndexError):
                continue

            try:
                results['story_quotes'][speaker].append(words)
            except KeyError:
                results['story_quotes'][speaker] = [words]

        elif 'Voting_Confessionals' in tbl.find_previous('span', {'class': 'mw-headline'}).attrs.get('id', ''):

            try:
                speaker, words = parse_quote_table(tbl)
            except (KeyError, ValueError, IndexError):
                continue

            try:
                results['voting_confessionals'][speaker].append(words)
            except KeyError:
                results['voting_confessionals'][speaker] = [words]
        elif 'Final_Words' in tbl.find_previous('span', {'class': 'mw-headline'}).attrs.get('id', ''):
            try:
                speaker, words = parse_quote_table(tbl)
            except (KeyError, ValueError, IndexError):
                continue

            results['final_words'][speaker] = words

    for b in soup.find_all('li'):
        if not(b.find_all_next('span', {'id': 'Still_in_the_Running'})) and (b.find_all_next('span', {'id': 'References'})):
            # trivia stage
            results['trivia'] += '* ' + b.text

    return pd.Series(results)


def find_episodes_for_season(season_type, season_name):
    srs_list = []

    url = season_url(season_type, season_name)
    season_r = requests.get(url)
    season_sp = bs4.BeautifulSoup(season_r.content, features="lxml")
    season_r.close()

    eps = extract_episode_urls_from_season_soup(season_sp)

    for ep in eps:
        ep_r = requests.get(ep)
        ep_sp = bs4.BeautifulSoup(ep_r.content, features="lxml")
        ep_r.close()

        extracted = extract_episode_info(ep_sp)
        extracted['wiki_link'] = ep
        extracted['season'] = season_name
        extracted['episode'] = os.path.basename(ep).replace('_', ' ')
        srs_list.append(extracted)

    return pd.DataFrame(srs_list)


def determine_ep_index(con):
    q = 'SELECT MAX(episode_id) + 1 FROM survivor.episode'
    index = con.execute(q).fetchall()[0][0]
    return index


def extract_new_episodes(con, asof=None):
    new_seasons = search_for_new_seasons(con, asof=asof)
    season_type = 'Survivor'  # for now, only considering American Survivor
    episodes_df = pd.DataFrame()

    for season_name in new_seasons:

        ep_df = find_episodes_for_season(season_type, season_name)

        print(season_name)
        print(ep_df)
        ep_df['season_id'] = ep_df['season'].apply(
            lambda x: get_season_id(con, x))
        episode_ids = ep_df[['episode', 'season_id']].apply(
            lambda x: get_ep_id(con, x[0], x[1]), axis=1)
        if episode_ids.isnull().any():
            new_idx = determine_ep_index(con)
            end_new_idx = new_idx + episode_ids.isnull().sum()
            new_series = np.arange(new_idx, end_new_idx)
            episode_ids.fillna(pd.Series(new_series), inplace=True)

        ep_df['episode_id'] = episode_ids

        episodes_df = pd.concat([episodes_df, ep_df])
    return episodes_df
