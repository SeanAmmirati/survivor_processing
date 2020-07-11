import pandas as pd


def pull_agg_contestant_stats(con):
    q = """
WITH ranked AS (
SELECT contestant_season_id, ROW_NUMBER() OVER(PARTITION BY cs.season_id ORDER BY njury DESC, ndays DESC) as placement
FROM survivor.contestant_season cs
LEFT JOIN (
SELECT contestant_id,  SUM(number_of_days_spent_in_episode) as ndays,
	COALESCE(sum(number_of_jury_votes), -1) njury
FROM survivor.episode_performance_stats eps
GROUP BY contestant_id
) eps
ON cs.contestant_season_id = eps.contestant_id
)

SELECT cs.contestant_season_id, c.first_name, c.last_name, cs.season_id,
SUM(number_of_days_spent_in_episode) as days_lasted,
MAX(v.medevac_quit) as quit,
MAX(v.medevac_quit) as med_evac,
MAX(v.votes_against) as votes_against,
MAX(ic.individual_wins) as individual_wins,
MAX(ranked.placement) as placement
FROM survivor.episode_performance_stats eps
LEFT JOIN survivor.contestant_season cs
on cs.contestant_season_id = eps.contestant_id
LEFT JOIN survivor.contestant c
ON cs.contestant_id = c.contestant_id
LEFT JOIN (SELECT voted_for_id,
		   COUNT(*) as votes_against,
		   CAST(SUM(CASE WHEN voted_for_id = contestant_id THEN 1 ELSE 0 END) > 0 AS INTEGER) as medevac_quit
	  FROM survivor.vote
	  GROUP BY voted_for_id) v
ON eps.contestant_id = v.voted_for_id
LEFT JOIN (SELECT contestant_id, SUM(percentage_of_win) as individual_wins
		   FROM survivor.immunity_challenge ic
		   WHERE team = 1
		   GROUP BY contestant_id) ic
ON ic.contestant_id = eps.contestant_id
LEFT JOIN ranked
ON ranked.contestant_season_id = eps.contestant_id
GROUP BY 1,2,3,4;
    """
    return pd.read_sql(sql=q, con=con)


def create_full_name_season_srs(con):
    df = pd.read_sql("""SELECT DISTINCT
                        CONCAT(c.first_name, ' ', c.last_name, ' ', cs.season_id) as match_id
                        , cs.contestant_season_id
                        FROM survivor.contestant_season cs
                        JOIN survivor.contestant c
                        ON c.contestant_id = cs.contestant_id""", con=con)
    return df.set_index('match_id')


def search_based_on_first_name_season_id(eng, first_name, season):
    first_name = first_name.lower()
    key = first_name + '_' + str(season)

    fmt_dict = dict(first_name=first_name, season_id=int(season))
    q = """
    SELECT cs.contestant_season_id
    FROM survivor.contestant_season cs
    JOIN survivor.contestant c
    ON cs.contestant_id = c.contestant_id
    JOIN survivor.season s
    ON cs.season_id = s.season_id
    WHERE LOWER(c.first_name) = '{first_name}'
    AND s.type = 'Survivor'
    AND s.season_id = {season_id}
    """.format(**fmt_dict)
    try:
        id_ = eng.execute(q).fetchall()[0][0]
    except IndexError:
        id_ = None
    return pd.Series([key, id_], index=['key', 'id'])


def create_full_name_srs(con):
    df = pd.read_sql("""SELECT DISTINCT
                        c.contestant_id,
                        CONCAT(c.first_name, ' ', c.last_name) as match_id
                        FROM survivor.contestant c""", con=con)
    return df.set_index('match_id')


def create_season_name_to_id(con):
    df = pd.read_sql("""SELECT season_id, name
                        FROM survivor.season""", con=con)
    return df.set_index('name')


def create_season_times(con):
    df = pd.read_sql(
        """SELECT season_id, showing_started, showing_ended
           FROM survivor.season
           WHERE type = 'Survivor'""", con=con)

    return df


def create_episode_times(con):
    df = pd.read_sql(
        """SELECT episode_id, firstbroadcast
           FROM survivor.episode e
           JOIN survivor.season s
           ON e.season_id = s.season_id
           WHERE s.type = 'Survivor'""", con=con
    )
    return df


def get_season_id_by_number_type(con, season_number, season_type):
    q = fr"""SELECT season_id FROM survivor.season
                    WHERE season_number = {season_number}
                    AND type = '{season_type}'"""
    q = q.replace('%', '%%')
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        res = None
    return res


def get_season_id(con, season_name):
    if pd.isna(season_name):
        return None
    q = "SELECT season_id FROM survivor.season WHERE name = '{season_name}'"
    res = con.execute(q).fetchall()[0][0]
    return res


def get_alliance_id(con, alliance_name, season_id):
    if pd.isna(alliance_name) or pd.isna(season_id):
        return None
    alliance_name = alliance_name.replace("'", "''")
    q = fr"""SELECT alliance_id FROM survivor.alliance
                    WHERE name = '{alliance_name}'
                    AND season_id = {season_id}"""
    q = q.replace('%', '%%')
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        res = None
    return res


def get_tribe_id(con, tribe_name, season_id):
    if pd.isna(tribe_name) or pd.isna(season_id):
        return None
    q = fr"""SELECT tribe_id FROM survivor.tribe
                    WHERE name = '{tribe_name}'
                    AND season_id = {int(season_id)}"""
    q = q.replace('%', '%%')
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        res = None
    return res


def get_attempt_number(con, contestant_season_id,
                       contestant_id):
    q = """SELECT attempt_number
           FROM survivor.contestant_season
           WHERE contestant_season_id = '{cont_id}'""".format(cont_id=int(contestant_season_id))

    print(contestant_season_id)
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        try:
            q_max = """SELECT MAX(attempt_number) + 1
                   FROM survivor.contestant_season
                   WHERE contestant_id = '{cont_id}'""".format(cont_id=int(contestant_id))
            res = con.execute(q_max).fetchall()[0][0]
        except (IndexError, ValueError):
            res = 1
    return res


def get_ep_id(con, episode_name, season_id):
    q = fr"""SELECT episode_id FROM survivor.episode
             WHERE episode_name = '{episode_name}' AND season_id = {season_id}"""
    q = q.replace('%', '%%')
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        res = None
    return res


def alter_episodes_between_geeks_wiki(episode, season):
    if season == 30:
        if episode > 1:
            episode -= 1
        if episode > 8:
            episode -= 1
    return episode


def get_ep_id_by_number(con, episode_number, season_id):
    if pd.isna(episode_number) or pd.isna(season_id):
        return None

    episode_number = alter_episodes_between_geeks_wiki(
        episode_number, season_id)
    q = fr"""SELECT episode_id FROM survivor.episode
             WHERE season_episode_number = {episode_number}
              AND season_id = {season_id}"""
    try:
        res = con.execute(q).fetchall()[0][0]
    except IndexError:
        res = None
    return res


def get_contestant_id(con, contestant_name, season_id):
    q = fr"""SELECT contestant_season_id FROM survivor.episode
             WHERE episode_name = '{episode_name}' AND season_id = {season_id}"""
    q = q.replace('%', '%%')
