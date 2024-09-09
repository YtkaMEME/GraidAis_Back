import pandas as pd


def merge_uploads(paths):
    uploads_dfs = [pd.read_csv(path, encoding='utf-8', sep=';', engine="python") for path in paths]
    main_df = pd.concat(uploads_dfs, ignore_index=True)
    return main_df
