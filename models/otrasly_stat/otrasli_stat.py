from models.otrasly_stat.config_otrasli import OTRASLI_TABLE
from utils import resources
from common import OracleDB
import re
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
                       where 1=1 and t.grp = :okved
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
                  res['okved'] = otrasly_df.grp[0]
                  res['okved_name'] = otrasly_df.ent_to_gr[0]
                  res['inn_count'] = otrasly_df.inn_count[0]
                  res['inn_count_change'] = otrasly_df.inn_count_change[0]
                  res['new_inn_count'] = otrasly_df.new_inn_count[0]
                  res['new_inn_count_percent'] = otrasly_df.new_inn_count_percent[0]
                  res['delete_inn_count'] = otrasly_df.delete_inn_count[0]
                  res['delete_inn_count_percent'] = otrasly_df.delete_inn_count_percent[0]
                  res['individual_share'] = otrasly_df.individual_share[0]
                  res['individual_share_change'] = otrasly_df.individual_share_change[0]
                  res['less_business_region'] = otrasly_df.less_business_region[0]
                  res['less_business_region_share'] = otrasly_df.less_business_region_share[0]
                  res['most_business_region'] = otrasly_df.most_business_region[0]
                  res['most_business_region_share'] = otrasly_df.most_business_region_share[0]
             elif otrasly_df.shape[0] > 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                  res[resources.RESPONSE_ERROR_FIELD] = 'Переданный ОКВЭД дублируется'
        else:
             res[resources.RESPONSE_STATUS_FIELD] = 'Error'
             res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный групп ОКВЭД'

        return res







