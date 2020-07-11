import requests
import bs4

import pandas as pd
import numpy as np

import re
from ..season.season_extract import season_url
from ..helpers.extract_helpers import search_for_new_seasons
from ..helpers.db_funcs import create_full_name_season_srs, get_season_id, create_full_name_srs, get_tribe_id, get_alliance_id


def create_contestant_url(first, last):
    first = first.replace(' ', '_')
    last = last.replace(' ', '_')
    return f'http://survivor.wikia.com/wiki/{first}_{last}'


def process_contestant_url(url):
    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.content, features="lxml")

    return extract_contestant_info(soup)


def extract_tribes(soup):
    tribe_links = soup.find_all(
        'div', {'data-source': 'tribes'})[0].find_all('a')
    return pd.Series([os.path.basename(x['href']) for x in tribe_links])


regex_dict = {
    'age': ['Age: (\d+)', 'Name \(Age\): .* \((\d+)\)', 'Name: .* \((\d+)\)', 'Current Age: (\d+)', 'birth date is (.*).', '\w+, (\d{2})$'],
    'hometown': ['Hometown: (.*)'],
    'current_residence': ['Current residence: (.*)'],
    'occupation': ['Occupation: (.*)'],
    'hobbies': ['Hobbies: (.*)'],
    'pet_peeves': ['Pet peeves: (.*)', 'Pet Peeves: (.*)'],
    'three_words': ['Three words to describe you: (.*)', '3 words to describe you: (.*)', '3 Words to Describe You: (.*)'],
    'claim_to_fame': ["What's your personal claim to fame\? (.*)", 'Personal claim to fame: (.*)'],
    'inspiration': ['Who or what is your inspiration in life\? (.*)', 'Inspiration in Life: (.*)'],
    'three_things': ['If you could have three things on the island, what would they be and why\? (.*)', ],
    'most_similar_self_reported': ['Which Survivor contestant are you most like\? (.*)', 'Survivor Contestant You Are Most Like: (.*)'],
    'reason': ['What\'s your reason for being on Survivor\? (.*)', 'Reason for Being on Survivor: (.*)'],
    'why_survive': ['Why do you think you\'ll "survive" Survivor\? (.*)', 'Why You Think You\'ll "Survive" Survivor: (.*)'],
    'previous_season': ['Previous Season: (.*)'],

}


def extract_contestant_info(soup):
    results = {}
    results['wiki_survivor_text'] = ''
    results['wiki_postsurvivor_text'] = ''
    results['trivia'] = ''

    # for the data profile on the rigth
    data_sources = ['birthdate', 'hometown',
                    'occupation', 'title',
                    'tribes', 'alliances',
                    'image']
    data_sources += [d + str(i) for i in range(2, 6) for d in data_sources]
    for i, src in enumerate(data_sources):

        srch = soup.find_all(['div', 'h2'], {'data-source': src})
        if srch:
            div = srch[0]
            if i < 3:
                res = list(div.children)[-2].text
            elif i == 3:
                res = div.text
            elif 'image' in src:
                res = [l for x, l in div.find_all(
                    'img')[0].attrs.items() if x == 'src'][0]
            else:
                res = [y.text for y in div.find_all('a')]
            if src == 'birthdate':
                try:
                    res = re.search('\((\d{4}-\d{2}-\d{2})\)', res).group(1)
                except:
                    pass
            if src == 'title':

                first, last = res.split(' ')
                results['first_name'] = first
                results['last_name'] = last
            else:
                results[src] = res

    for i, p in enumerate(soup.find_all('p')):
        if 'This section is empty' in p.text:
            continue
        if p.find_all_next('span', {'id': ['Australian_Survivor', 'Survivor',
                                           'Survivor_South_Africa', 'Survivor_New_Zealand']}):

            # We are in the profile section
            not_found = []
            lines = p.text.split('\n')
            for j, line in enumerate(lines):
                any_found = False
                for k, regex_list in regex_dict.items():
                    for regex in regex_list:
                        m = re.search(regex, line)

                        if m:
                            results[k] = m.group(1)
                            any_found = True
                            break
                if not any_found:
                    not_found.append(line)
            results['other_profile'] = '\n'.join(not_found)
        elif p.find_all_next('span', {'id': 'Post-Survivor'}):
            # we are in the Survivor stage
            results['wiki_survivor_text'] += p.text
        elif p.find_all_next('span', {'id': 'Trivia'}):
            # we are in the Post-Survivor srage
            results['wiki_postsurvivor_text'] += p.text
        elif p.find_all_next('span', {'id': 'References'}):
            # trivia stage
            results['trivia'] += p.text

    results['sex'] = 'M' if len(
        [x.text for x in soup.find_all('a') if 'Male' in x.text]) else 'F'

    return pd.Series(results)


# Tribe

def tribe_url(tribe):
    return f'https://survivor.fandom.com/wiki/{tribe}'


def process_tribe(tribe):
    url = tribe_url(tribe)
    soup = bs4.BeautifulSoup(requests.get(url).content, features="lxml")
    srs = extract_tribe_info(soup)
    return srs


def extract_tribe_info(soup):
    results = {}

    results['summary'] = ''
    results['tribal_history'] = ''
    results['trivia'] = ''

    keys = ['tribenameorigin', 'tribetype', 'dayformed',
            'opponents', 'status', 'lowestplacingmember',
            'highestplacingmember', 'insigniaimage',
            'flagimage', 'buffimage', 'image', 'season']
    for k in keys:
        srch = soup.find_all(['div', 'figure'], {'data-source': k})

        if srch:
            div = srch[0]

            if 'image' in k:
                ret = [l for x, l in div.find_all(
                    'img')[0].attrs.items() if x == 'src'][0]
            elif k != 'opponents':
                ret = div.text.split('\n')[-2]

            else:
                ret = [y.text for y in div.find_all('a')]

            results[k] = ret

    for i, p in enumerate(soup.find_all('p')):
        if 'This section is empty' in p.text:
            continue
        if p.find_all_next('span', {'id': 'Members'}):

            # We are in the summary section
            results['summary'] += p.text
        elif p.find_all_next('span', {'id': 'Tribal_History'}):
            # we are in the members section, skip this
            pass
        elif p.find_all_next('span', {'id': 'Gallery'}):
            # we are in the tribal history
            results['tribal_history'] += p.text

    for b in soup.find_all('li'):
        if not(b.find_all_next('span', {'id': 'Gallery'})) and (b.find_all_next('span', {'id': 'References'})):
            # trivia stage
            results['trivia'] += '* ' + b.text

    return pd.Series(results)


# Alliances
def alliance_url(alliance):
    return f'https://survivor.fandom.com/wiki/{alliance}'


def process_alliance(alliance):
    url = tribe_url(alliance)
    soup = bs4.BeautifulSoup(requests.get(url).content, features="lxml")
    srs = extract_alliance_info(soup)
    return srs


def extract_alliance_info(soup):
    results = {}

    results['summary'] = ''
    results['history'] = ''
    results['trivia'] = ''

    keys = ['founder', 'dayformed',
            'opponents', 'lowestplacingmember',
            'highestplacingmember', 'image', 'season']
    for k in keys:
        srch = soup.find_all(['div', 'figure'], {'data-source': k})

        if srch:
            div = srch[0]

            if 'image' in k:
                ret = [l for x, l in div.find_all(
                    'img')[0].attrs.items() if x == 'src'][0]
            elif k != 'opponents':
                ret = div.text.split('\n')[-2]

            else:
                ret = [y.text for y in div.find_all('a')]

            results[k] = ret

    for i, p in enumerate(soup.find_all('p')):
        if 'This section is empty' in p.text:
            continue
        if p.find_all_next('span', {'id': 'Members'}):

            # We are in the summary section
            results['summary'] += p.text
        elif p.find_all_next('span', {'id': 'History'}):
            # we are in the members section, skip this
            pass
        elif p.find_all_next('span', {'id': 'Trivia'}):
            # we are in the tribal history
            results['history'] += p.text

    for b in soup.find_all('li'):
        if not(b.find_all_next('span', {'id': 'History'})) and (b.find_all_next('span', {'id': 'Gallery'})):
            # trivia stage
            results['trivia'] += '* ' + b.text

    return pd.Series(results)


def extract_contestant_names_from_season_soup(soup):

    contestants = []

    season_summary = soup.find_all('span', attrs={'id': 'Castaways'})[0]

    cursor = season_summary

    while cursor.find_all_next('span', attrs={'id': 'Season_Summary'}):
        cursor = cursor.find_next('td')

        # Number column
        next_a_attrs = cursor.find_next('a').attrs
        if 'class' in next_a_attrs:
            if 'image-thumbnail' in next_a_attrs['class']:
                contestant_name = next_a_attrs['title']

            contestants.append(contestant_name)

    contestants = list(set(contestants))
    return contestants


def find_contestants_for_season(season_type, season_name):
    srs_list = []

    url = season_url(season_type, season_name)
    season_r = requests.get(url)
    season_sp = bs4.BeautifulSoup(season_r.content, features="lxml")
    season_r.close()

    cs = extract_contestant_names_from_season_soup(season_sp)

    for c in cs:
        c_url = create_contestant_url(*c.split(' '))
        extracted = process_contestant_url(c_url)
        extracted['wikia'] = c_url
        extracted['season_id'] = season_name
        srs_list.append(extracted)

    return pd.DataFrame(srs_list)


def determine_contestant_season_index(con):
    q = 'SELECT MAX(contestant_season_id) + 1 FROM survivor.contestant_season'
    index = con.execute(q).fetchall()[0][0]
    return index


def determine_contestant_index(con):
    q = 'SELECT MAX(contestant_id) + 1 FROM survivor.contestant'
    index = con.execute(q).fetchall()[0][0]
    return index


def determine_tribe_index(con):
    q = 'SELECT MAX(tribe_id) + 1 FROM survivor.tribe'
    index = con.execute(q).fetchall()[0][0]
    return index


def determine_alliance_index(con):
    q = 'SELECT MAX(alliance_id) + 1 FROM survivor.alliance'
    index = con.execute(q).fetchall()[0][0]
    return index


def alter_contestant_name(contestant_name):

    if 'Spradlin ' in contestant_name:
        contestant_name = contestant_name.replace('Spradlin', 'Spradlin-Wolfe')

    return contestant_name


def extract_contestants(con, asof=None):
    new_seasons = search_for_new_seasons(con, asof=asof)
    season_type = 'Survivor'  # for now, only considering American Survivor
    contestants_df = pd.DataFrame()

    name_to_id_season = create_full_name_season_srs(con).iloc[:, 0].to_dict()
    name_to_id = create_full_name_srs(con).iloc[:, 0].to_dict()
    for season_name in new_seasons:

        contestant_df = find_contestants_for_season(season_type, season_name)
        contestant_df['season_id'] = contestant_df['season_id'].apply(
            lambda x: get_season_id(con, x))

        merge_keys = contestant_df['first_name'] \
            + ' ' \
            + contestant_df['last_name'] \
            + ' ' \
            + contestant_df['season_id'].astype(str)

        merge_keys = merge_keys.apply(alter_contestant_name)

        contestant_season_ids = merge_keys.map(name_to_id_season)

        if contestant_season_ids.isnull().any():
            new_idx = determine_contestant_season_index(con)
            end_new_idx = new_idx + contestant_season_ids.isnull().sum()
            new_series = np.arange(new_idx, end_new_idx)
            idx_rel = contestant_season_ids[contestant_season_ids.isnull(
            )].index
            contestant_season_ids.fillna(
                pd.Series(new_series, index=idx_rel), inplace=True)

        merge_keys_2 = merge_keys.apply(lambda x: ' '.join(x.split(' ')[:-1]))

        contestant_ids = merge_keys_2.map(name_to_id)
        if contestant_ids.isnull().any():
            new_idx = determine_contestant_index(con)
            end_new_idx = new_idx + contestant_ids.isnull().sum()
            new_series = np.arange(new_idx, end_new_idx)
            contestant_ids.fillna(pd.Series(new_series), inplace=True)

        contestant_df['contestant_season_id'] = contestant_season_ids
        contestant_df['contestant_id'] = contestant_ids

        contestants_df = pd.concat([contestants_df, contestant_df])

    u_tribes = find_unique_tribes(contestants_df)
    u_tribes = list(set([t.replace('► ', '') for t in u_tribes]))

    u_alliances = find_unique_alliances(contestants_df)

    tribal_df = pd.DataFrame([process_tribe(t) for t in u_tribes])
    tribal_df['name'] = u_tribes

    tribal_df['season_id'] = tribal_df['season'].apply(
        lambda x: get_season_id(con, x))

    tribal_df['tribe_id'] = tribal_df[['name', 'season_id']].apply(
        lambda x: get_tribe_id(con, *x), axis=1)

    if tribal_df['tribe_id'].isnull().any():
        null_bool = tribal_df['tribe_id'].isnull()
        new_idx = determine_tribe_index(con)
        end_new_idx = new_idx + null_bool.sum()
        full_id = pd.Series(np.arange(new_idx, end_new_idx),
                            index=tribal_df[null_bool].index)
        tribal_df['tribe_id'] = tribal_df['tribe_id'].fillna(full_id)

    alliances_df = pd.DataFrame([process_alliance(a) for a in u_alliances])
    alliances_df['name'] = u_alliances
    alliances_df['season_id'] = alliances_df['season'].apply(
        lambda x: get_season_id(con, x))

    alliances_df['alliance_id'] = alliances_df[['name', 'season_id']].apply(
        lambda x: get_alliance_id(con, *x), axis=1)

    if alliances_df['alliance_id'].isnull().any():
        null_bool = alliances_df['alliance_id'].isnull()
        new_idx = determine_alliance_index(con)
        end_new_idx = new_idx + null_bool.sum()
        full_id = pd.Series(np.arange(new_idx, end_new_idx),
                            index=alliances_df[null_bool].index)
        alliances_df['alliance_id'] = alliances_df['alliance_id'].fillna(
            full_id)

    return contestants_df, tribal_df, alliances_df


def find_unique_multiple_columns(df, columns):
    ret = set()

    for col in columns:
        l_of_l = df[col].tolist()
        for l in l_of_l:
            if isinstance(l, list):
                ret = ret.union(set(l))

    return list(ret)


def find_unique_tribes(df):
    tribe_cols = df.columns[df.columns.str.contains('tribes')]
    return find_unique_multiple_columns(df, tribe_cols)


def find_unique_alliances(df):
    alliance_cols = df.columns[df.columns.str.contains('alliances')]
    return find_unique_multiple_columns(df, alliance_cols)
