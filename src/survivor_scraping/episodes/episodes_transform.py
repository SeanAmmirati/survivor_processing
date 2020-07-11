from copy import deepcopy
import pandas as pd
from ..helpers.transform_helpers import add_to_df, process_viewership
from ..helpers.db_funcs import create_full_name_season_srs, create_season_name_to_id


def process_episode_number(df, *args, **kwargs):
    extracted_ep_number = df['episodenumber'].str.extract(
        '(\d+)\/\d+ ?\(?(\d+)?\)?')
    extracted_ep_number.columns = [
        'season_episode_number', 'overall_episode_number']
    extracted_ep_number['season_episode_number'] = \
        extracted_ep_number['season_episode_number'].astype(float)
    extracted_ep_number['overall_episode_number'] = \
        extracted_ep_number['overall_episode_number'].astype(float)

    return extracted_ep_number


def process_first_broadcast(df, *args, **kwargs):
    return pd.to_datetime(df['firstbroadcast'])


def process_share(df, *args, **kwargs):
    share_extract = df['share'].str.extract(
        '(\d+\.?\d*)\/?(\d+\.?\d*|Unavailable|N\/A)\[?.?\]?')
    share_extract.columns = ['survivor_rating', 'overall_slot_rating']
    overall_slot_rating = share_extract['overall_slot_rating'].apply(
        lambda x: {'Unavailable': None, 'N/A': None}.get(x, x)).astype(float)
    survivor_rating = share_extract['survivor_rating'].astype(float)
    return pd.concat([overall_slot_rating, survivor_rating], axis=1)


def process_season_id(df, season_to_id, *args, **kwargs):
    return pd.DataFrame(df['season'].map(season_to_id.iloc[:, 0].to_dict()))


def alter_key(k):

    k = k.replace('Rodger', 'Roger')
    k = k.replace('Brkich', 'Mariano')
    k = k.replace(',', '')
    k = k.replace('JoAnna', 'Joanna')
    k = k.replace(' (page does not exist)', '')
    k = k.replace('Brooke Sturck', 'Brooke Struck')
    k = k.replace('Ruth Marie', 'Ruth-Marie')
    k = k.replace('Candice Cody', 'Candice Woodcock')
    k = k.replace('Amber Mariano 15', 'Parvati Shallow 15')
    k = k.replace('Rupert Bonheam 15', 'Rupert Boneham 15')
    k = k.replace('Matt Elrod 29', 'Matthew Elrod 29')
    k = k.replace('Russell Hants 29', 'Russell Hantz 29')
    k = k.replace('Roxy Morris 35', 'Roxanne Morris 35')
    k = k.replace('RC Saint-Amour 35', 'R.C. Saint-Amour 35')
    k = k.replace('Sierra Dawn Thomas 30', 'Sierra Thomas 30')
    k = k.replace('Sierra Dawn Thomas 10', 'Sierra Thomas 10')
    k = k.replace('Carl Boudreax 43', 'Carl Boudreaux 43')
    k = k.replace('Geoffrey Cooke-Tonneson 38', 'Geoffrey Cooke-Tonnesen 38')
    k = k.replace('Carl Boudreax 43', 'Carl Boudreaux 43')
    k = k.replace('Garrett Adlestein 2', 'Garrett Adelstein 2')
    k = k.replace('Natalie Azoqa 43', 'Natalia Azoqa 43')
    k = k.replace('Ted Rogers 12', 'Ted Rogers Jr. 12')
    k = k.replace('Deena Benett 3', 'Deena Bennett 3')
    k = k.replace('Christine Shields Markoski 34',
                  'Christine Shields-Markoski 34')
    k = k.replace('Lisi Whelchel 35', 'Lisa Whelchel 35')
    k = k.replace('Hayden 16', 'Hayden Moss 16')
    k = k.replace('Roark Lustin 20', 'Roark Luskin 20')
    k = k.replace('Christian Hubucki 43', 'Christian Hubicki 43')
    k = k.replace('Kelley wentworth 45', 'Kelley Wentworth 45')
    k = k.replace('Sandra Diaz-Twine 41', 'Sandra Diaz-Twine 40')
    k = k.replace('Rob Mariano 41', 'Rob Mariano 33')
    k = k.replace('Kim Spradlin-Wolfe 40', 'Kim Spradlin 40')

    if k == 'Survivor: Palau 25':
        k = None
    elif 'Jeff Probst' in k:
        k = None
    elif 'Tree Mail' in k:
        k = None
    return k


def create_dict_based_df(x, name_dict, col='voting_confessionals',
                         show_mistakes=False):
    dic = x[col]
    ep = x['episode_id']
    season = x['season_id']
    return_dict = {'id': [], 'content': [], 'season': [], 'episode_id': []}

    for k, v in dic.items():
        key = k + ' ' + str(season)
        # for use in wiki
        if show_mistakes:
            key_init = deepcopy(key)
        key = alter_key(key)

        if show_mistakes:
            if key_init != key:
                print('Changed key')
                print(x['wiki_link'])
                print('In section')
                print(col)
                print("From person")
                print(key)
                print('Changed from')
                print(key_init)

        if key is None:
            id_contestant = 'General'
        else:
            id_contestant = name_dict[key]
        if not isinstance(v, list):
            v = [v]
        for vl in v:
            return_dict['id'].append(id_contestant)
            return_dict['content'].append(vl)
            return_dict['season'].append(season)
            return_dict['episode_id'].append(ep)
    return pd.DataFrame(return_dict)


def create_vc_df(x, name_dict):
    return create_dict_based_df(x, name_dict)


def create_fw_df(x, name_dict):
    return create_dict_based_df(x, name_dict, 'final_words')


def create_sq_df(x, name_dict):
    return create_dict_based_df(x, name_dict, 'story_quotes')


def create_full_dict_df(ep_df, name_mapping, contestants, df_creator):
    name_dict = name_mapping.iloc[:, 0].to_dict()
    meta_dfs = ep_df.apply(lambda x: df_creator(x,
                                                name_dict), axis=1)
    ret_df = pd.concat(meta_dfs.tolist())
    return ret_df


def create_full_vc_df(ep_df, name_mapping, contestants):
    extract_pattern = ('(\(((vot(ing|es) (against|for) (.+?))|'
                       '(changes (vote) to (.+?))|(voting to (kidnap) (.+?)))\) )?(.*)')

    initial_vc_df = create_full_dict_df(
        ep_df, name_mapping, contestants, create_vc_df)
    expanded_content = initial_vc_df['content'].str.extract(extract_pattern)
    vc_content_columns = expanded_content.apply(create_vc_cols, axis=1)
    initial_vc_df.drop(columns='content', inplace=True)
    vc_df = pd.concat([initial_vc_df, vc_content_columns], axis=1)
    vc_df['recipient_id'] = vc_df[['person', 'season']].dropna().apply(
        lambda x: match_to_contestant_season(*x, contestants=contestants),
        axis=1)
    vc_df.rename(columns={'id': 'voter_id'}, inplace=True)
    vc_df.drop(columns='person', inplace=True)
    return vc_df


def create_full_sq_df(ep_df, name_mapping, contestants):

    sq_df = create_full_dict_df(ep_df, name_mapping, contestants, create_sq_df)
    sq_df.rename(columns={'id': 'contestant_id'}, inplace=True)
    return sq_df


def create_full_fw_df(ep_df, name_mapping, contestants):
    fw_df = create_full_dict_df(ep_df, name_mapping, contestants, create_fw_df)
    fw_df.rename(columns={'id': 'contestant_id',
                          'person': 'contestant_id'}, inplace=True)

    return fw_df


def create_vc_cols(x):
    ret_list = []
    if x[5]:
        type_of_vote = 'vote'
        initial_or_changed = 'initial'
        for_or_against = x[4]
        person = x[5]
        content = x[12]
    elif x[8]:
        type_of_vote = 'vote'
        initial_or_changed = 'changed'
        for_or_against = 'against'
        person = x[8]
        content = x[12]
    elif x[11]:
        type_of_vote = 'kidnap'
        initial_or_changed = None
        for_or_against = None
        person = x[11]
        content = x[12]
    else:
        type_of_vote = 'vote'
        initial_or_changed = None
        for_or_against = None
        person = None
        content = x[12]

    ret_list = [type_of_vote, initial_or_changed,
                for_or_against, person, content]
    ret_srs = pd.Series(ret_list, index=[
                        'type_of_vote', 'initial_or_changed',
                        'for_or_against', 'person', 'content'])
    return ret_srs


def match_to_contestant_season(name, season, contestants):

    name = clean_vc_innername(name, season)
    contest = contestants[(contestants['season_id'] == season) & (
        contestants['first_name'] == name)]
    if len(contest) == 1:
        return contest['contestant_season_id'].iloc[0]
    else:

        if len(contest) == 0:
            n_split = name.split(' ')
            name
            if len(n_split) > 1:
                contest = contestants[(contestants['season_id'] == season) &
                                      (contestants['first_name'] == n_split[0]) &
                                      (contestants['last_name'].str[0] == n_split[1][0])]
            if len(contest) == 1:
                return contest['contestant_season_id'].iloc[0]
            else:
                import pdb
                pdb.set_trace()

    return None


def clean_vc_innername(name, season):
    name = name.replace(', impersonating Casey Kasem', '')
    name = name.replace(' twice', '')
    name = name.replace('; first vote', '')

    name = name.replace('Rodger', 'Roger')
    name = name.replace('JoAnna', 'Joanna')
    name = name.replace('Ruth Marie', 'Ruth-Marie')
    name = name.replace('Roxy', 'Roxanne')

    name = name.replace('Syliva', 'Sylvia')
    name = name.replace('Cochran', 'John')

    name = name.replace('RC', 'R.C.')
    if 'Maria' not in name:
        name = name.replace('Abi', 'Abi-Maria')

    if season == 29:
        name = name.replace('Matt', 'Matthew')

    if season == 21:
        if name == 'Russell':
            name = 'Russell H.'
    return name


def transform_episode_df(ep_df, season_to_id, name_mapping, contestants):

    processing_columns = {
        ('season_episode_number', 'overall_episode_number'): process_episode_number,
        ('firstbroadcast',): process_first_broadcast,
        ('overall_slot_rating', 'survivor_rating'): process_share,
        ('viewership',): process_viewership,
        ('season_id',): process_season_id
    }
    added = add_to_df(ep_df, processing_columns,
                      inplace=False, season_to_id=season_to_id)

    vc_df = create_full_vc_df(added, name_mapping, contestants)
    fw_df = create_full_fw_df(added, name_mapping, contestants)
    sq_df = create_full_sq_df(added, name_mapping, contestants)

    drop_columns = ['voting_confessionals', 'final_words',
                    'story_quotes', 'share', 'episodenumber', 'season']

    added.drop(columns=drop_columns, inplace=True)

    rename_columns = {'episode': 'episode_name'}

    added.rename(columns=rename_columns, inplace=True)

    return added, vc_df, fw_df, sq_df


def transform_new_episodes(ep_df, eng):
    season_to_id = create_season_name_to_id(eng)
    name_mapping = create_full_name_season_srs(eng)
    contestants = pd.read_sql('''SELECT c.first_name, c.last_name, cs.season_id, cs.contestant_season_id
                                 FROM survivor.contestant_season cs
                                 JOIN survivor.contestant c
                                 ON cs.contestant_id = c.contestant_id''', con=eng)
    transformed = transform_episode_df(
        ep_df, season_to_id, name_mapping, contestants)

    return transformed
