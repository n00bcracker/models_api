from models.advisor.config_advisor import ADVISOR_TABLE, ADVISOR_EVENTS_TABLE
from utils import resources, check_inn
from common import OracleDB
import re
import numpy as np
import datetime
import calendar
import pandas as pd
#import sqlalchemy as sa


def check_kpp(kp):
    if kp != None: 
                            try:
                                 kpp = str(kp)
                                 digits = [int(x) for x in kpp]
                            except: 
                                return False
                            else:
                                if len(digits)==9: 
                                                    return True
                                else: return False 
    else: return True

def check_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except:
        return False

def calc_past_date(hday):    
        today = datetime.datetime.today()
        pday = today.replace(day=min(today.day, calendar.monthrange(hday.year, today.month)[1]), year=hday.year)
        delta = pday - hday
        if delta.days >= 0:
            prevday = today.replace(day=min(today.day, calendar.monthrange(hday.year-1, today.month)[1]), year=hday.year-1)
            nextday = pday
            delta1 = hday - prevday
            delta2 = nextday - hday
            if delta1.days <= delta2.days:
                nday = prevday
                clbr_year = today.year - 1
            else:
                nday = nextday
                clbr_year = today.year
        else:
            prevday = pday
            nextday = today.replace(day=min(today.day, calendar.monthrange(hday.year+1, today.month)[1]), year=hday.year+1)
            delta1 = hday - prevday
            delta2 = nextday - hday
            if delta1.days <= delta2.days:
                nday = prevday
                clbr_year = today.year
            else:
                nday = nextday
                clbr_year = today.year + 1
        return nday, clbr_year
    

class AdvisorStat(OracleDB):

    advisor_table_name = ADVISOR_TABLE
    advisor_events_table_name = ADVISOR_EVENTS_TABLE

    def generate_res_data(self, data_ful, fio, events_row = None, event_is_empty = 1):
        res = {}
        congrats = {}
     
        products = [
          
            'salary_project',          
            'business_cards',       
            'credit',   
            'deposit',          
            'acquiring',        
            'guarantee',      
            'nso',         
            'ved',       
            'sms',      
            'encashment',           
            'mobile_banking'
        ]

        # блок праздники
      
        if ( data_ful.isnull().birth_lpr_fio[0] == False and  
             data_ful.birth_lpr_fio[0].lower() == fio.lower() and
             data_ful.isnull().birthday[0] == False and             
             data_ful.birthday[0]<=calc_past_date(data_ful.birthday[0])[0] <= data_ful.birthday[0] + pd.DateOffset(4)
           ):
            congrats['birthday'] = {
                'code': 'BIRTHDAY',
                'fullName': data_ful.birth_lpr_fio[0],
                'date': data_ful.birthday[0].strftime('%Y-%m-%d'),
                'congYear': calc_past_date(data_ful.birthday[0])[1]
            }
   
        if ( data_ful.isnull().company_anniversary[0] == False and 
             data_ful.isnull().company_count_year[0] == False and 
             data_ful.company_anniversary[0] - pd.DateOffset(5)<=calc_past_date(data_ful.company_anniversary[0])[0] <= data_ful.company_anniversary[0] + pd.DateOffset(6)
           ):
            congrats['company_anniversary'] = {
                'code': 'COMPANY_ANNIVERSARY',
                'date': data_ful.company_anniversary[0].strftime('%Y-%m-%d'), 
                'fullYearsFromDate': data_ful.company_count_year[0],
                'congYear': calc_past_date(data_ful.company_anniversary[0])[1]
            }
            
        if ( data_ful.isnull().client_anniversary[0] == False and
             data_ful.isnull().client_count_year[0] == False and 
             data_ful.client_anniversary[0] - pd.DateOffset(5)<=calc_past_date(data_ful.client_anniversary[0])[0] <= data_ful.client_anniversary[0] + pd.DateOffset(6)
           ):
            congrats['client_anniversary'] = {
                'code': 'CLIENT_ANNIVERSARY',
                'date': data_ful.client_anniversary[0].strftime('%Y-%m-%d'),
                'fullYearsFromDate': data_ful.client_count_year[0],
                'congYear': calc_past_date(data_ful.client_anniversary[0])[1]
            }
        
        if ( data_ful.isnull().industry_name[0] == False and 
             data_ful.isnull().industry_day[0] == False and 
             data_ful.industry_day[0] - pd.DateOffset(5)<=calc_past_date(data_ful.industry_day[0])[0] <= data_ful.industry_day[0] + pd.DateOffset(6)
           ):
            congrats['industry_day'] = {
                'code': 'INDUSTRY_DAY',
                'indName': data_ful.industry_name[0],
                'date': data_ful.industry_day[0].strftime('%Y-%m-%d'),
                'congYear': calc_past_date(data_ful.industry_day[0])[1]
            }
        # прогноз блокировок
        block_reas_res = []
        for i in [
                   'block_reason_1',
                   'block_reason_2',
                   'block_reason_3',
                   'block_reason_4',
                   'block_reason_5',
                   'block_reason_6',
                   'block_reason_7',
                   'block_reason_8'
                  ]:
            if data_ful.isnull()[i][0] == False: block_reas_res.append(data_ful[i][0])
        if block_reas_res: res['blockingReasons'] = block_reas_res
                
        # прогноз изменения стоимости бизнеса
        if data_ful.isnull().same_business_change_value[0] == False:
            res['sameBusinnessChangeValue'] =  data_ful.same_business_change_value[0]
        
        # добавление продуктов
        prod_to_res = []
        for i in products:
            if data_ful[i][0] == 1: prod_to_res.append(i.upper())
        if prod_to_res: res['productsOffer'] = prod_to_res

        # переход на новый ПУ (для нового предложения update res[offers].update)
        if data_ful.isnull().change_pu_new[0] == False & data_ful.isnull().change_pu_profit[0] == False:
            res['offers'] = {
                'tariff':
                    {                  
                        'puNew': data_ful.change_pu_new[0],
                        'profit': data_ful.change_pu_profit[0]
                    }
            }
        if data_ful.isnull().themes_appeal_1[0] == False & data_ful.isnull().themes_appeal_2[0] == False & \
                data_ful.isnull().themes_appeal_3[0] == False:
            res['information'] = [int(data_ful.themes_appeal_1[0]), int(data_ful.themes_appeal_2[0]), int(data_ful.themes_appeal_3[0])]
        
        # проверяем что ивентс не пусто
        if event_is_empty:
            
                    if congrats: res['congratulations'] = congrats.pop(list(congrats)[0])
        else:
                
                # удаляем отключенные клиентом поздравления 
                if congrats:
                    for i in list(congrats):
                        
                        if events_row[i][0]!= None and calc_past_date(datetime.datetime.strptime(congrats[i]['date'], '%Y-%m-%d'))[1] == events_row[i][0].year: congrats.pop(i, None)

                # выбираем самое приоритетное и добавляем в ответ res
                order = {
                    'birthday': 4,
                    'company_anniversary': 3,
                    'client_anniversary': 2,
                    'industry_day': 1    
                }
                if congrats: 
                        max_order = 0
                        name_max_order = ''
                        for i in congrats.keys():
                                   if order[i] > max_order: 
                                                            name_max_order = i
                                                            max_order = order[i]
                        congrats = congrats[name_max_order]
                        res['congratulations'] = congrats     
                 
        return res
#gdljg
    def get_data(self, inn, kpp, table_name):
        if kpp==None:  
                       sql_query = """                                                                       
                                       select * 
                                       from {tablename} t
                                       where 1=1 and t.inn = :inn and t.kpp is null 
                                   """
                       sql_query = sql_query.format(tablename=table_name)
                       sovet_df = self.read_sql_query(sql_query, params=[inn,])
                       
        else:   
                sql_query = """                                                                       
                               select * 
                               from {tablename} t
                               where 1=1 and t.inn = :inn and t.kpp = :kpp
                          """
                sql_query = sql_query.format(tablename=table_name)
                sovet_df = self.read_sql_query(sql_query, params=[inn, kpp])

        return sovet_df

    def insert_event(self, inn, kpp):
        data = {}
        current_date = datetime.datetime.today().strftime('%Y.%m.%d')
        if kpp == None:
                    data['cols_names'] = 'inn, insert_date'
                    data['values_names'] = ":inn, to_date(:current_date, 'yyyy-mm-dd')"
                    self.insert_data_to_table(self.advisor_events_table_name, data, [inn, current_date]) 
        else: 
                data['cols_names'] = 'inn, kpp, insert_date'
                data['values_names'] = ":inn, :kpp, to_date(:current_date, 'yyyy-mm-dd')"
                self.insert_data_to_table(self.advisor_events_table_name, data, [inn, kpp, current_date]) 
                  
    def post_update(self, inn, kpp, congrat, date):                    
        res = {}
        data = {}
        
        if (
            inn != None and 
            check_inn(inn) and  
            check_kpp(kpp) and  
            congrat in ['BIRTHDAY','COMPANY_ANNIVERSARY','CLIENT_ANNIVERSARY', 'INDUSTRY_DAY'] and
            check_date(date)
           ):   
                   
            try:
                 a = self.get_row_block(inn, kpp, self.advisor_events_table_name) 
                  
            except:  
                     res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                     res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Не удалось проверить наличие клиента в таблице!'
            else: 
                 
                if a['is_here'] == 1: 
                        current_date = datetime.datetime.today().strftime('%Y.%m.%d')
                        data['set_query'] = congrat+"= to_date(:cong_date,'yyyy-mm-dd'), " + "update_date = to_date(:current_date,'yyyy-mm-dd')"
                        if kpp==None: 
                                data['where_query'] = 'inn = :inn and kpp is null'     
                                try:        
                                     self.update_data_in_table(self.advisor_events_table_name, data, [date, current_date, inn]) 

                                except: 
                                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                        res[resources.RESPONSE_ERROR_FIELD] = 'Не удалось обновить данные в таблице!'
                                else: 
                                        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                                        res[resources.RESPONSE_ERROR_FIELD] = 'Данные успешно обновлены!'
                        else: 
                                    data['where_query'] = 'inn = :inn and kpp = :kpp'
                                    try:        
                                        self.update_data_in_table(ADVISOR_EVENTS_TABLE, data, [date, current_date, inn, kpp]) 

                                    except: 
                                            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                            res[resources.RESPONSE_ERROR_FIELD] = 'Не удалось обновить данные в таблице!'
                                    else: 
                                            res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                                            res[resources.RESPONSE_ERROR_FIELD] = 'Данные успешно обновлены!'
                                            
                else:       
                         res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                         res[resources.RESPONSE_ERROR_FIELD] = 'Клиент не в таблице или дубли или None. Ошибка БД!'
                        
                            
        else: 
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Неправильный формат входных данных или отсутствие обязательных полей!'            
        return res
    

    def get_block_predict(self, inn, kpp, fio):
        res = dict()
                
        if inn != None and fio != None and check_inn(inn) and check_kpp(kpp):
            try: 
                advisor_dict = self.get_row_block(inn, kpp, self.advisor_table_name)
                event_dict = self.get_row_block(inn, kpp, self.advisor_events_table_name)
            except:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Ошибка обращения к БД'
                    res['advisor_events_table_name'] = self.advisor_events_table_name
            else:
                if event_dict['is_here']==2 or advisor_dict['is_here']==2: 
                                                     res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                                     res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Дублирование, None в БД'
                else:
                    if   event_dict['is_here']==0 and advisor_dict['is_here']==0: res['show'] = False
                    elif event_dict['is_here']==1 and advisor_dict['is_here']==0: res['show'] = True
                    elif event_dict['is_here']==0 and advisor_dict['is_here']==1: 
                         
                            
                            try:
                                b = self.generate_res_data(advisor_dict['df_row'], fio)
                              
                            except: 
                                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Обработка данных витрины советника!'
                            else:
                                try: 
                                    if b: self.insert_event(inn, kpp)
                                except: 
                                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                    res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Добавление данных в таблицу событий!'
                                else: 
                                    res.update(b)
                                    if b: res['show'] = True
                                    else: res['show'] = False
                    
                    elif event_dict['is_here']==1 and advisor_dict['is_here']==1:
                                                   
                        try:
                            res.update(self.generate_res_data(advisor_dict['df_row'], fio, events_row=event_dict['df_row'], event_is_empty=0))
                        except: 
                                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Обработка данных витрины советника и табл. сост.'
                        else: res['show'] = True
                    else:  
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Такого не может быть'
                                                                                             
        else:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Внешняя ошибка. Некорректный ИНН или КПП или ФИО'
    
        return res
    
    def get_row_block(self, inn, kpp, table_name):       
        
        res = dict()    
        df = self.get_data(inn, kpp, table_name)
        
        if df is None:
                             res['is_here']  = 2 
                             
        elif df.shape[0] == 0:               
                               res['is_here'] = 0 
                                      
        elif df.shape[0] == 1:                   
                               res['is_here'] = 1
                               res['df_row'] = df

        elif df.shape[0] > 1:
                               res['is_here'] = 2 
        
        else: 
             res['is_here'] = 2
          
        return res













