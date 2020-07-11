from ..helpers.load_helpers import upsert


def load_episodes(transformed_dfs, eng):
    table_names = ['episode', 'voting_confessional',
                   'final_words', 'story_quotes']
    conflict_cols = ['episode_id',
                     'voter_id, season, episode_id, content',
                     'contestant_id, season, episode_id, content',
                     'season, episode_id, content']

    for i, tbl in enumerate(table_names):

        df = transformed_dfs[i]
        conf_list = conflict_cols[i].split(', ')
        upsert(df, eng, tbl, conf_list)
