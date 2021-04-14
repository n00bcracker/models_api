from models.advisor.config_advisor import ADVISOR_TABLE, ADVISOR_EVENTS_TABLE
from utils import resources, check_inn
from common import OracleDB
import re
import numpy as np
import datetime
#import pandas as pd
#import sqlalchemy as sa


def check_client_key(inn, kpp):

    return True

class AdvisorStat(OracleDB):

    advisor_table_name = ADVISOR_TABLE
    advisor_events_table_name = ADVISOR_EVENTS_TABLE

    def generate_res_data(self, data_ful):
        res = {}
        congrats = {}

        products = {
            'zpp_project_fl': 'SALARY_PROJECT',
            'corp_card_fl': 'BUSINESS_CARDS',
            'credit_fl': 'CREDIT',
            'deposit_fl': 'DEPOSIT',
            'acquiring_fl': 'ACQUIRING',
            'garanty_fl': 'GUARANTEE',
            'nso_fl': 'NSO',
            'ved_fl': 'VED',
            'sms_fl': 'SMS',
            'inkass_fl': 'ENCASHMENT',
            'mob_bank_fl': 'MOBILE_BANKING'
        }

        # блок праздники
        if data_ful.isnull().birth_lpr_fio[0] == False & data_ful.isnull().birth_lpr_day[0] == False:
            congrats['b_day'] = {
                'code': 'BIRTHDAY',
                'fullName': data_ful.birth_lpr_fio[0],
                'date': data_ful.birth_lpr_day[0]
            }
        if data_ful.isnull().birth_work_day[0] == False & data_ful.isnull().birth_work_count_year[0] == False:
            congrats['work_in_bank'] = {
                'code': 'CLIENT_ANNIVERSARY',
                'date': data_ful.birth_work_day[0],
                'fullYearsFromDate': data_ful.birth_work_count_year[0]
            }

        if data_ful.isnull().birth_biz_day[0] == False & data_ful.isnull().birth_biz_count_year[0] == False:
            congrats['reg_bis_day'] = {
                'code': 'COMPANY_ANNIVERSARY',
                'date': data_ful.birth_biz_day[0],
                'fullYearsFromDate': data_ful.birth_biz_count_year[0]
            }
        if data_ful.isnull().prof_name[0] == False & data_ful.isnull().prof_day[0] == False:
            congrats['industry_day'] = {
                'code': 'INDUSTRY_DAY',
                'indName': data_ful.prof_name[0],
                'date': data_ful.prof_day[0]
            }
        # прогноз блокировок
        if data_ful.block_ib_fl[0] == 1:
            res['blockingReason'] = True
        else:
            res['blockingReason'] = False

        # добавление продуктов
        prod_to_res = []
        for i in products.keys():
            # print(type(i), i)
            if data_ful[i][0] == 1: prod_to_res.append(products[i])
        if prod_to_res: res['productsOffer'] = prod_to_res

        # переход на новый ПУ (для нового предложения update res[offers].update)
        if data_ful.isnull().change_pu_old[0] == False & data_ful.isnull().change_pu_new[0] == False & \
                data_ful.isnull().change_pu_profit[0] == False:
            res['offers'] = {
                'tariff':
                    {
                        'puOld': data_ful.change_pu_old[0],
                        'puNew': data_ful.change_pu_new[0],
                        'profit': data_ful.change_pu_profit[0]
                    }
            }
        if data_ful.isnull().themes_appeal_1[0] == False & data_ful.isnull().themes_appeal_2[0] == False & \
                data_ful.isnull().themes_appeal_3[0] == False:
            res['information'] = [data_ful.themes_appeal_1[0], data_ful.themes_appeal_2[0], data_ful.themes_appeal_3[0]]

        res['congratulations'] = congrats

        return res

    def get_data(self, inn, kpp):
        sql_query = """                                                                       
                       select * 
                       from {tablename} t
                       where 1=1 and t.inn = :inn and t.kpp = :kpp
                    """
        sql_query = sql_query.format(tablename=self.advisor_table_name)
        sovet_df = self.read_sql_query(sql_query, params=[inn, kpp])

        return sovet_df

    def insert_event(self, inn, kpp, congrat):
        res_data = {}
        current_date = datetime.datetime.today().strftime('%Y.%m.%d')
        sql_query = '''insert into {table_name}(inn, kpp, {congr_type}, insert_date) values(inn:inn, kpp:kpp, {congr_type}: current_date, insert_date:current_date);
                       commit;'''
        sql_query = sql_query.format(self.advisor_events_table_name, congrat)
        self.read_sql_query(self, sql_query, params=[inn, kpp, current_date])
        res_data['inn'] = inn
        res_data['kpp'] = kpp
        res_data['congrat'] = congrat
        return res_data

    def get_block_predict(self, inn, kpp):
        res = dict()
        if check_client_key(inn, kpp):
             sovet_df = self.get_data(inn, kpp)
             if sovet_df is None:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
             elif sovet_df.shape[0] == 0:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ключа'
             elif sovet_df.shape[0] == 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Ok'

                  #for i in sovet_df.columns[sovet_df.isna().any()==False].tolist()[1:]:
                  #    if i == 'BIRTHDAY_LPR': res['CONGRATULATIONS'] =
                  #      if sovet_df[i].dtypes == 'int64': res[i] = float(sovet_df[i][0])
                  #      else: res[i] = sovet_df[i][0]

                  res.update(self.generate_res_data(sovet_df))
                  
             elif sovet_df.shape[0] > 1:
                  res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                  res[resources.RESPONSE_ERROR_FIELD] = 'Переданный ключ дублируется'
        else:
             res[resources.RESPONSE_STATUS_FIELD] = 'Error'
             res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ключ'

        return res













