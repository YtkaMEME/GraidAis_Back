import pandas as pd

from GraidAis_Back.Data_base.DataBase import DataBase
from datetime import datetime


def merge_uploads_def(file_names, old_db_table=None):
    dataframes = []

    if old_db_table is not None and not old_db_table.empty:
        dataframes.append(old_db_table)
    for file_name in file_names:
        df = None
        if file_name.endswith('.csv'):
            df = pd.read_csv(f"{file_name}")
        elif file_name.endswith('.xlsx'):
            df = pd.read_excel(f"{file_name}")
        dataframes.append(df)

    marged = pd.concat(dataframes)
    marged = marged.astype(str).replace('nan', '')

    def update_value(old_value, new_value):
        return new_value if new_value != '' else old_value

    def combine_rows(group):
        result = group.iloc[0].copy()
        for _, row in group.iterrows():
            for col in group.columns:
                result[col] = update_value(result[col], row[col])
        return result

    combined_df = marged.groupby('ID пользователя', as_index=False).apply(lambda x: combine_rows(x))

    combined_df['Последнее обновление'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    first_column = combined_df.pop('Последнее обновление')
    combined_df.insert(0, 'Последнее обновление', first_column)

    return combined_df


def update_db(paths, new_base, db_name):
    db = DataBase(db_name)
    try:
        if new_base:
            all_data = merge_uploads_def(paths)
        else:
            old_data = db.get_table("people")
            all_data = merge_uploads_def(paths, old_data)
        db.create_db(all_data, "people")
        db.close()
    except Exception as e:
        print(f"Ошибка при обновлении базы данных: {str(e)}", flush=True)
        return {"error": f"Ошибка при обновлении базы данных"}
    return
