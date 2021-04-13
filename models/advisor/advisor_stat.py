from models.advisor.config_advisor import ADVISOR_TABLE, ADVISOR_EVENTS_TABLE
from utils import resources, check_inn
from common import OracleDB
import re
import numpy as np
import datetime
#import pandas as pd
#import sqlalchemy as sa


def check_client_key(client_key):

    return True

class AdvisorStat(OracleDB):

    advisor_table_name = ADVISOR_TABLE
    advisor_events_table_name = ADVISOR_EVENTS_TABLE

    def get_data(self, client_key):
        sql_query = """                                                                       
                       select * 
                       from {tablename} t
                       where 1=1 and t.client_key = :client_key
                    """
        sql_query = sql_query.format(tablename=self.advisor_table_name)
        sovet_df = self.read_sql_query(sql_query, params=[client_key, ])

        return sovet_df

    def insert_event(self, inn, kpp, congrat):
        res_data = {}
        current_date = datetime.datetime.today().strftime('%Y.%m.%d')
        sql_query = '''insert into {table_name}(inn, kpp, {congr_type}, insert_date) values(inn:inn, kpp:kpp, {congr_type}: current_date, insert_date:current_date);
                       commit;'''
        sql_query = sql_query.format(self.advisor_events_table_name, congrat)
        self.read_sql_query(self, sql_query, params=[inn, kpp, current_date,])
        res_data['inn'] = inn
        res_data['kpp'] = kpp
        res_data['congrat'] = congrat
        return res_data

    def get_block_predict(self, client_key):
        res = dict()
        if check_client_key(client_key):
             sovet_df = self.get_data(client_key)
             if sovet_df is None:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
             elif sovet_df.shape[0] == 0:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ключа'
             elif sovet_df.shape[0] == 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Ok'               
                  for i in sovet_df.columns[sovet_df.isna().any()==False].tolist()[1:]:
                      if i == 'BIRTHDAY_LPR': res['CONGRATULATIONS'] =
                        if sovet_df[i].dtypes == 'int64': res[i] = float(sovet_df[i][0])
                        else: res[i] = sovet_df[i][0]
                  
             elif sovet_df.shape[0] > 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                  res[resources.RESPONSE_ERROR_FIELD] = 'Переданный ключ дублируется'
        else:
             res[resources.RESPONSE_STATUS_FIELD] = 'Error'
             res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ключ'

        return res










