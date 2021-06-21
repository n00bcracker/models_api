from models.otrasly_stat.config_otrasli import OTRASLI_TABLE
from utils import resources
from common import OracleDB
import re
import numpy as np
#import pandas as pd
#import sqlalchemy as sa


def check_okv(okved):
    if re.fullmatch('\d{2}', okved) or re.fullmatch('\d{2}.\d{1,2}', okved):
        return True
    else:
        return False

class OtrasliStat(OracleDB):

    otrasli_table_name = OTRASLI_TABLE

    def get_data(self, okved):
        sql_query = """                                                  
                       select * 
                       from {tablename} t 
                       where 1=1 and t.okved = :okved
                    """
        sql_query = sql_query.format(tablename=self.otrasli_table_name)
        otrasl_df = self.read_sql_query(sql_query, params=[okved, ])

        #conn_str = 'oracle+cx_oracle://{}:{}@{}'
        #conn_str = conn_str.format('FEDOSEEV_SI[PXU_DCBUL_UMA_MOD]', 'Qa12347$', 'DATALAB')
        #oracle_db = sa.create_engine(conn_str, encoding='utf-8', max_identifier_length = 128)
        #conn = oracle_db.connect()
        #otrasl_df = pd.read_sql_query(sql_query, conn, params=[okved,])
        #conn.close()
        # print(dataframe)

        return otrasl_df


    def get_block_predict(self, okv):
        res = dict()
        if check_okv(okv):
             otrasly_df = self.get_data(okv)
             if otrasly_df is None:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
             elif otrasly_df.shape[0] == 0:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ОКВЭД'
             elif otrasly_df.shape[0] == 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Ok'               
                  for i in otrasly_df.columns[otrasly_df.isna().any()==False].tolist()[1:]:
                        if otrasly_df[i].dtypes == 'int64': res[i] = float(otrasly_df[i][0])
                        else: res[i] = otrasly_df[i][0]
                  
             elif otrasly_df.shape[0] > 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                  res[resources.RESPONSE_ERROR_FIELD] = 'Переданный ОКВЭД дублируется'
        else:
             res[resources.RESPONSE_STATUS_FIELD] = 'Error'
             res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный групп ОКВЭД'

        return res







