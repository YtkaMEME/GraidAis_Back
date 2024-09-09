import sqlite3

import pandas as pd


class Data_Base:
    def __init__(self, db_name):
        self.name = db_name
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def create_db(self, df, table_name):
        df.to_sql(table_name, self.connection, if_exists='replace', index=False)

    def close(self):
        self.connection.close()

    def __drop_table(self, table_name):
        sql_query = f"DROP table if exists {table_name}"
        self.cursor.execute(sql_query)
        self.connection.commit()

    def get_table(self, table_name, num):
        sql_query = f"SELECT * FROM {table_name}"
        self.cursor.execute(sql_query)
        table = self.cursor.fetchall()
        column_names = [description[0] for description in self.cursor.description]
        df = pd.DataFrame(table, columns=column_names)
        df = df.fillna(" ")
        return df.head(num)

    def get_list_table_colums(self, table_name):
        sql_query = f"PRAGMA table_info({table_name})"
        self.cursor.execute(sql_query)
        table_colums = self.cursor.fetchall()
        colums = [table_colum[1] for table_colum in table_colums]
        return colums

    def keyword_search(self, table_name, filters):
        import itertools

        all_filters = filters.get('allFilters', {})
        keys = list(all_filters.keys())
        values = list(all_filters.values())

        for r in range(len(keys), 0, -1):
            for combination in itertools.combinations(zip(keys, values), r):
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
