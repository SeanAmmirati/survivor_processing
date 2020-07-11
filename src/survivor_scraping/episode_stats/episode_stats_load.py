from ..helpers.load_helpers import upsert


def load_episode_stats(transformed_dfs, eng):
    dict_keys_to_tables = {
        'tribal_council': 'vote',
        'reward_challenge': 'reward_challenge',
        'immunity_challenge': 'immunity_challenge',
        'overall_episode': 'episode_performance_stats'
    }

    dict_keys_to_conflicts = {
        'tribal_council': 'season_id, episode_id, tc_number, contestant_id, vote_number',
        'reward_challenge': 'tc_number, season_id, contestant_id, episode_id, challenge_number',
        'immunity_challenge': 'tc_number, season_id, contestant_id, episode_id',
        'overall_episode': 'episode_id, contestant_id, season_id'
    }

    for k, df in transformed_dfs.items():
        table_name = dict_keys_to_tables[k]
        conflict_cols = dict_keys_to_conflicts[k]
        conf_list = conflict_cols.split(', ')
        upsert(df, eng, table_name, conf_list)
