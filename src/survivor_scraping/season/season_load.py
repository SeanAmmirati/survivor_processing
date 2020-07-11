from ..helpers.load_helpers import upsert


def load_new_seasons(transformed_seasons, eng):
    upsert(transformed_seasons, eng, 'season', ['season_id'])
