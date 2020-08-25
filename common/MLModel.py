from config import ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_TNS
from config import METAFILES_DIR
import os
import sqlalchemy as sa
import pandas as pd
import joblib
import pickle
import traceback

class MLModel():
    def __init__(self):
        # self.app = app
        #TODO Сделать логирование
        self.sql_auth_data = {
                                'login' : ORACLE_USERNAME,
                                'password' : ORACLE_PASSWORD,
                                'tns' : ORACLE_TNS
                            }

        if not os.path.isdir(METAFILES_DIR):
            os.mkdir(METAFILES_DIR)

    def predict(self, *args, **kwargs):
        pass

    def fit(self, *args, **kwargs):
        pass

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
            # self.app.logger.error(f"Во время исполнения SQL-запроса произошла ошибка: \n{errors}")
            return None
        else:
            return dataframe

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
            return True
        except Exception:
            error = traceback.format_exc()
            print(f"Во время сохранения SQL-таблицы произошла ошибка: \n{error}")
            # self.app.logger.error(f"Во время сохранения SQL-таблицы произошла ошибка: \n{errors}")
            return False

    def load_joblib_file(self, filename):
        try:
            file = joblib.load(filename)
        except FileNotFoundError:
            error = traceback.format_exc()
            print(f'Отсутствует файл "{filename}".\n{error}\n')
            # self.app.logger.error(f'Отсутствует файл "{filename}".\n{errors}\n')
            return None
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise
        else:
            return file

    def save_object_in_joblib(self, object, filename):
        try:
            joblib.dump(object, filename, compress=5, protocol=pickle.DEFAULT_PROTOCOL)
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise

    def load_pickle_table(self, filename):
        try:
            df = pd.read_pickle(filename)
        except FileNotFoundError:
            error = traceback.format_exc()
            print(f'Отсутствует файл "{filename}".\n{error}\n')
            # self.app.logger.error(f'Отсутствует файл "{filename}".\n{errors}\n')
            return None
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise
        else:
            return df

    def save_df_in_pickle(self, df, filename):
        try:
            df.to_pickle(filename, protocol=pickle.DEFAULT_PROTOCOL)
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise

