from ..helpers.load_helpers import upsert


def load_confessionals(transformed_df, eng):
    table = 'confessional'
    conf_list = ['day', 'n_in_episode', 'episode_id', 'contestant_id']

    upsert(transformed_df, eng, table, conf_list)
