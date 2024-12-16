import sqlite3

import pandas as pd


class Data_Base:
    def __init__(self, db_name):
        self.name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

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

    def keyword_search(self, table_name, all_filters):
        import itertools

        keys = list(all_filters.keys())
        values = list(all_filters.values())

        for r in range(len(keys), 0, -1):
            for combination in itertools.combinations(zip(keys, values), r):
                if any(k == "Поиск" for k, v in combination):
                    continue
                condition = " AND ".join([f"[{k}]='{v}'" for k, v in combination])
                sql_query = f"SELECT * FROM [{table_name}] WHERE {condition}"
                self.cursor.execute(sql_query)
                result = self.cursor.fetchall()

                if result:
                    column_names = [description[0] for description in self.cursor.description]
                    df = pd.DataFrame(result, columns=column_names)
                    df = df.fillna(" ")
                    return df

        return self.get_table(table_name, 100)

    def get_unique_elements(self, table_name):
        sql_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(sql_query)
        table = self.cursor.fetchall()
        column_names = [description[0] for description in self.cursor.description]
        df = pd.DataFrame(table, columns=column_names)
        df = df.fillna(" ")

        unique_elements_dict = {}

        for column in df.columns:
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
        mask = df.apply(lambda row: row.astype(str).str.contains(query, case=False, na=False)).any(axis=1)
        return df[mask]

    # Метод для выполнения SQL-запросов (INSERT, UPDATE, DELETE)
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
