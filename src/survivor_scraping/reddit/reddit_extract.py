import requests
from datetime import datetime
import pandas as pd


def api_request(subreddit, start_date, end_date, sub_or_comment='submission'):
    fmt_dict = dict(sub_or_comment=sub_or_comment,
                    after_ts=int(start_date.timestamp()),
                    before_ts=int(end_date.timestamp()),
                    subreddit=subreddit)
    return ('https://api.pushshift.io/reddit/{sub_or_comment}/search/'
            '?after={after_ts}'
            '&before={before_ts}'
            '&sort=asc&subreddit={subreddit}&limit=1000').format(**fmt_dict)


def get_comment_id_url(submission_id):
    return 'https://api.pushshift.io/reddit/submission/comment_ids/{submission_id}'.format(submission_id=submission_id)


def get_comment_ids(submission_id):
    url = get_comment_id_url(submission_id)
    r = requests.get(url)
    ids = r.json()['data']
    r.close()
    return ids


def extract_comment_data(comment_id):
    return 'https://api.pushshift.io/reddit/comment/search/?ids={comment_id}'.format(comment_id=comment_id)


def extract_comment_df(submission_id):
    comment_ids = get_comment_ids(submission_id)

    str_add = ','.join(comment_ids)

    comment_url = extract_comment_data(str_add)
    r = requests.get(comment_url)
    df = pd.DataFrame(r.json()['data'])
    return df


def create_reddit_df(start_date=datetime(2000, 1, 1),
                     end_date=datetime(2020, 1, 1)):

    types = ['submission', 'comment']

    dfs = [pd.DataFrame() for t in types]

    iteration = 0

    for i, t in enumerate(types):

        max_date = start_date

        while max_date < end_date:
            iteration += 1
            url = api_request('survivor', max_date, end_date, sub_or_comment=t)
            r = requests.get(url)
            if not r:
                break
            new_data = pd.DataFrame(r.json()['data'])
            dfs[i] = pd.concat([dfs[i], new_data], ignore_index=True)
            if len(new_data) == 0:
                break

            max_date = pd.to_datetime(
                new_data['created_utc'].max(), unit='s')

    return dfs


def extract_reddit(eng, asof=None, stop=None):
    if asof is None:
        q = 'SELECT MAX(created_dt) FROM survivor.reddit_submissions'
        asof = eng.execute(q).fetchall()[0][0]

    if stop is None:
        stop = datetime.now()

    stop = pd.to_datetime(stop)
    asof = pd.to_datetime(asof)

    dfs = create_reddit_df(start_date=asof, end_date=stop, )
    return dfs
