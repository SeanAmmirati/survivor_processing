from ..helpers.load_helpers import upsert


def load_new_reddit_dfs(transformed_dfs, eng):
    table_list = ['submissions', 'comments']
    table_list = ['reddit_' + t for t in table_list]

    pk_list = [['id'], ['id', 'retrieved_on']]
    for i, tbl in enumerate(table_list):
        df = transformed_dfs[i]
        # df = push_alterations(df)
        upsert(df, eng, tbl, ['id'])


# def push_alterations(df):
#     missing_cols = ['approved_at_utc', 'banned_at_utc', 'view_count',
#                     'event_end', 'event_start', 'category',
#                     'content_categories', 'removal_reason', 'updated_utc',
#                     'user_removed', 'asso'
#                     ]
#     missing_types = [float, float, float,
#                      float, float, float,
#                      float, float, float,
#                      bool
#                      ]

#     for col, t in zip(missing_cols, missing_types):
#         df[col] = None
#         df[col] = df[col].astype(t)
#     return df
