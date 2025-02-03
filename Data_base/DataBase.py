import sqlite3

import pandas as pd
import re


class DataBase:
    def __init__(self, db_name):
        self.name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def check_table(self, table_name):
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not self.cursor.fetchone():
            return False

        return True

    def create_db(self, df, table_name):
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%dT%TZ")
        df = df.astype(str)

        df.to_sql(table_name, self.connection, if_exists='replace', index=False)

    def close(self):
        self.connection.close()

    def __drop_table(self, table_name):
        sql_query = f"DROP table if exists {table_name}"
        self.cursor.execute(sql_query)
        self.connection.commit()

    def insert_user(self, username, hashed_password):
        query = "INSERT INTO users (username, password) VALUES (?, ?)"
        self.execute_query(query, (username, hashed_password))

    def get_user(self, username):
        query = "SELECT * FROM users WHERE username = ?"
        self.cursor.execute(query, (username,))
        return self.cursor.fetchone()

    def delete_user(self, username):
        query = "DELETE FROM users WHERE username = ?"
        self.cursor.execute(query, (username,))
        self.connection.commit()

    def get_table(self, table_name, num = 0):

        if not self.check_table(table_name):
            return pd.DataFrame()

        sql_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(sql_query)
        table = self.cursor.fetchall()
        column_names = [description[0] for description in self.cursor.description]
        df = pd.DataFrame(table, columns=column_names)
        df = df.fillna(" ")
        if num == 0:
            return df
        else:
            return df.head(num)

    def get_list_table_colums(self, table_name):
        sql_query = f"PRAGMA table_info({table_name})"
        self.cursor.execute(sql_query)
        table_colums = self.cursor.fetchall()
        colums = [table_colum[1] for table_colum in table_colums]
        return colums

    def filers(self, table_name, all_filters):
        import itertools

        keys = list(all_filters.keys())
        values = list(all_filters.values())

        def parse_filter_value(value):
            """
            Разделяет значения фильтров по запятым и пробелам и возвращает список уникальных значений.
            """
            # Убираем лишние пробелы, разделяем по запятым и пробелам
            split_values = re.split(r'[,\s]+', value.strip())
            # Убираем пустые строки и возвращаем уникальные значения
            return list(filter(None, split_values))

        for r in range(len(keys), 0, -1):
            for combination in itertools.combinations(zip(keys, values), r):
                if any(k == "Поиск" for k, v in combination):
                    continue

                conditions = []
                for k, v in combination:
                    if k == 'ВозрастMIN':
                        # Фильтрация по возрастному диапазону
                        age_min = v
                        age_max = all_filters.get('ВозрастMAX', None)
                        if age_max:
                            conditions.append(f"[Возраст] BETWEEN {age_min} AND {age_max}")
                    elif k == 'ВозрастMAX':
                        # Пропускаем 'ВозрастMAX', он уже обработан выше
                        continue
                    else:
                        # Для остальных фильтров
                        parsed_values = parse_filter_value(v)
                        if len(parsed_values) > 1:
                            # Если несколько значений, используем IN
                            conditions.append(f"[{k}] IN ({','.join([repr(val) for val in parsed_values])})")
                        else:
                            # Если одно значение, обычное равенство
                            conditions.append(f"[{k}]='{parsed_values[0]}'")

                if conditions:
                    condition = " AND ".join(conditions)
                    sql_query = f"SELECT * FROM [{table_name}] WHERE {condition}"
                    self.cursor.execute(sql_query)
                    result = self.cursor.fetchall()

                    if result:
                        column_names = [description[0] for description in self.cursor.description]
                        df = pd.DataFrame(result, columns=column_names)
                        df = df.fillna(" ")
                        return df

        return self.get_table(table_name, 100)

    def get_unique_elements(self, table_name, columns):

        if not self.check_table(table_name):
            return {}

        sql_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(sql_query)
        table = self.cursor.fetchall()

        column_names = [description[0] for description in self.cursor.description]
        df = pd.DataFrame(table, columns=column_names)
        df = df.fillna(" ")

        unique_elements_dict = {}

        for column in columns:
            if column in df.columns:
                unique_elements_dict[column] = df[column].unique().tolist()

        return unique_elements_dict

    def create_users_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.connection.commit()
            print("Таблица users успешно создана.")
        except sqlite3.Error as e:
            print(f"Ошибка при создании таблицы: {e}")

    def full_text_search(self, df, query):
        query = re.sub(r'\s+(AND|OR)\s+', r' \1 ', query)

        def apply_condition(query_part):
            sub_conditions = query_part.split(' AND ')
            sub_mask = pd.Series(True, index=df.index)

            for sub_cond in sub_conditions:
                sub_mask &= df.apply(
                    lambda row: row.astype(str).str.contains(sub_cond.strip(), case=False, na=False)).any(axis=1)

            return sub_mask

        or_conditions = query.split(' OR ')

        final_mask = pd.Series(False, index=df.index)

        for condition in or_conditions:
            condition = condition.strip('() ')
            condition_mask = apply_condition(condition)
            final_mask |= condition_mask

        return df[final_mask]

    def execute_query(self, query, params=None):
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"Ошибка выполнения запроса: {e}")
            return False
        return True
