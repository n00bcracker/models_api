from config import ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_TNS

import sqlalchemy as sa
import pandas as pd
import traceback

class OracleDB:
    def __init__(self):
        self.sql_auth_data = {
            'login' : ORACLE_USERNAME,
            'password' : ORACLE_PASSWORD,
            'tns' : ORACLE_TNS
        }

    def read_sql_table(self, table_name, schema=None):
        conn_str = 'oracle+cx_oracle://{}:{}@{}'
        conn_str = conn_str.format(self.sql_auth_data['login'], self.sql_auth_data['password'],
                                   self.sql_auth_data['tns'])
        try:
            oracle_db = sa.create_engine(conn_str, encoding='utf-8', max_identifier_length=128)

            connection = oracle_db.connect()
            dataframe = pd.read_sql_table(table_name, connection, schema=schema)
            connection.close()
        except Exception:
            error = traceback.format_exc()
            print(f"Во время загрузки SQL-таблицы произошла ошибка: \n{error}")
            # self.app.logger.error(f"Во время загрузки SQL-таблицы произошла ошибка: \n{errors}")
            return None
        else:
            return dataframe

    def read_sql_query(self, sql_query, params=None):
        conn_str = 'oracle+cx_oracle://{}:{}@{}'
        conn_str = conn_str.format(self.sql_auth_data['login'], self.sql_auth_data['password'],
                                   self.sql_auth_data['tns'])
        try:
                oracle_db = sa.create_engine(conn_str, encoding='utf-8', max_identifier_length=128)

                conn = oracle_db.connect()
                dataframe = pd.read_sql_query(sql_query, conn, params=params)
                conn.close()
        except Exception:
            error = traceback.format_exc()
            print(f"Во время исполнения SQL-запроса произошла ошибка: \n{error}")
            return None
        else:
            return dataframe

    def execute_sql_query(self, sql_query, params=None):
        conn_str = 'oracle+cx_oracle://{}:{}@{}'
        conn_str = conn_str.format(self.sql_auth_data['login'], self.sql_auth_data['password'],
                                   self.sql_auth_data['tns'])

        try:
            oracle_db = sa.create_engine(conn_str, encoding='utf-8', max_identifier_length=128)

            conn = oracle_db.connect()
            conn.execute(sql_query, params)
            conn.close()
        except Exception:
            error = traceback.format_exc()
            print(f"Во время исполнения SQL-запроса произошла ошибка: \n{error}")
            return False
        else:
            return True

    def save_df_in_sql_table(self, df, dtype, table_name, schema=None):
        conn_str = 'oracle+cx_oracle://{}:{}@{}'
        conn_str = conn_str.format(self.sql_auth_data['login'], self.sql_auth_data['password'],
                                   self.sql_auth_data['tns'])
        try:
            oracle_db = sa.create_engine(conn_str, encoding='utf-8', max_identifier_length=128)

            connection = oracle_db.connect()
            df.to_sql(table_name, schema=schema, con=connection, if_exists='append', dtype=dtype, index=False,
                      chunksize=500)
            connection.close()
        except Exception:
            error = traceback.format_exc()
            print(f"Во время сохранения SQL-таблицы произошла ошибка: \n{error}")
            # self.app.logger.error(f"Во время сохранения SQL-таблицы произошла ошибка: \n{errors}")
            return False
        else:
            return True
        
        
        
        
        
        
        