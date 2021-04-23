from models.advisor.config import ADVISOR_TABLE, ADVISOR_EVENTS_TABLE
from models.advisor.config import CONGRATULATION_CODES, BLOCKING_REASONS, PRODUCT_OFFERS, TARIFFS
from utils import resources, check_inn, check_kpp
from common import OracleDB
import datetime
import calendar
import pandas as pd

class AdvisorStat(OracleDB):
    advisor_tablename = ADVISOR_TABLE
    advisor_events_tablename = ADVISOR_EVENTS_TABLE

    def __init__(self):
        super().__init__()

    @staticmethod
    def check_congr_code(congr_code):
        if congr_code in CONGRATULATION_CODES:
            return True
        else:
            return False

    @staticmethod
    def check_block_reason(block_reason):
        if block_reason in BLOCKING_REASONS:
            return True
        else:
            return False

    @staticmethod
    def check_prod_offer(prod_offer):
        if prod_offer in PRODUCT_OFFERS:
            return True
        else:
            return False

    @staticmethod
    def check_tariff_name(tariff_name):
        if tariff_name in TARIFFS:
            return True
        else:
            return False

    @staticmethod
    def days_to_holiday(hday):
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

        delta_nearest_day = hday - nday
        return delta_nearest_day.days, clbr_year

    def get_advisor_suggestions(self, inn, kpp=None):
        sql_query = f"""
                        select
                            t.inn, t.kpp,
                            t.birth_lpr_fio, t.birthday,
                            t.industry_name, t.industry_day,
                            t.company_anniversary, t.company_count_year,
                            t.client_anniversary, t.client_count_year,
                            t.block_reason_1, t.block_reason_2, t.block_reason_3, t.block_reason_4,
                            t.block_reason_5, t.block_reason_6, t.block_reason_7, t.block_reason_8,
                            t.same_business_change_value,
                            t.salary_project, t.business_cards, t.credit, t.depoit, t.acquiring, t.gurantee, t.nso,
                            t.ved, t.sms, t.encashment, t.mobile_banking,
                            t.change_pu_new, t.change_pu_profit,
                            t.themes_appeal_1, t.themes_appeal_2, t.themes_appeal_3
                                from {self.advisor_tablename} t
                                    where 1=1
                                    and t.inn = :inn
                    """

        adv_sugg_df = self.read_sql_query(sql_query, params=[inn, ])
        if adv_sugg_df is not None and kpp is not None:
            adv_sugg_df = adv_sugg_df.loc[adv_sugg_df.kpp == kpp, :]
        return adv_sugg_df

    def get_user_state(self, inn, kpp=None):
        sql_query = f"""
                        select
                            t.inn,
                            t.kpp,
                            t.birthday,
                            t.company_anniversary,
                            t.client_anniversary,
                            t.industry_day
                                from {self.advisor_events_tablename} t
                                    where 1=1
                                    and t.inn = :inn
                    """

        user_state_df = self.read_sql_query(sql_query, params=[inn,])
        if user_state_df is not None and kpp is not None:
            user_state_df = user_state_df.loc[user_state_df.kpp == kpp, :]
        return user_state_df

    def update_user_state(self, inn, data, kpp=None):
        sql_query = """
                        update {tablename}
                            set {values_clause}
                                where 1=1
                                and inn = :inn
                                {kpp_clause}
                            """

        values_clause = ', '.join([k + ' = :' + k for k in data.keys()])
        params = data
        if kpp is None:
            sql_query = sql_query.format(tablename=self.advisor_events_tablename, values_clause=values_clause,
                                         kpp_clause='')
        else:
            params['kpp'] = kpp
            sql_query = sql_query.format(tablename=self.advisor_events_tablename, values_clause=values_clause,
                                         kpp_clause='kpp = :kpp')

        update_success = self.execute_sql_query(sql_query, params=params)
        return update_success

    def actual_suggestions_transform(self, adv_sugg_df, user_state_df, lpr_fullname):
        adv_sugg = adv_sugg_df.iloc[0]

        actual_suggestions = dict()
        #Модуль поздравлений
        congratulations= list()
        # День рождения ЛПР
        if pd.notnull(adv_sugg.birth_lpr_fio) and adv_sugg.birth_lpr_fio.lower() == lpr_fullname.lower():
            days2hday, clbr_year = self.days_to_holiday(adv_sugg.birthday)
            if days2hday >= -3 and days2hday <= 0: # В течение 3х дней после ДР
                congratulations.append({
                    'code' : 'BIRTHDAY',
                    'date' : adv_sugg.birthday.strftime('%Y-%m-%d'),
                    'celebrationYear' : clbr_year,
                })

        # День регистрации бизнеса
        if pd.notnull(adv_sugg.company_anniversary):
            days2hday, clbr_year = self.days_to_holiday(adv_sugg.company_anniversary)
            if days2hday >= -5 and days2hday <= 5:
                congratulations.append({
                    'code': 'COMPANY_ANNIVERSARY',
                    'date': adv_sugg.company_anniversary.strftime('%Y-%m-%d'),
                    'celebrationYear': clbr_year,
                    'fullYearsFromDate' : adv_sugg.client_count_year,
                })

        # День отрасли
        if pd.notnull(adv_sugg.industry_name):
            days2hday, clbr_year = self.days_to_holiday(adv_sugg.industry_day)
            if days2hday >= -5 and days2hday <= 5:
                congratulations.append({
                    'code': 'INDUSTRY_DAY',
                    'indName' : adv_sugg.industry_name,
                    'date': adv_sugg.industry_day.strftime('%Y-%m-%d'),
                    'celebrationYear': clbr_year,
                })

        # День начала обслуживания в Банке
        if pd.notnull(adv_sugg.client_anniversary):
            days2hday, clbr_year = self.days_to_holiday(adv_sugg.client_anniversary)
            if days2hday >= -5 and days2hday <= 5:
                congratulations.append({
                    'code': 'CLIENT_ANNIVERSARY',
                    'date': adv_sugg.industry_day.strftime('%Y-%m-%d'),
                    'celebrationYear': clbr_year,
                    'fullYearsFromDate': adv_sugg.client_count_year,
                })

        if len(congratulations) > 0:
            actual_suggestions['congratulations'] = congratulations[0]

        # Модуль предложений по продуктам
        products_offers = list()
        for product_name in PRODUCT_OFFERS:
            if adv_sugg[product_name.lower()] == 1:
                products_offers.append(product_name)

        if len(products_offers) > 0:
            actual_suggestions['productsOffer'] = products_offers

        # Модуль выгодных предложений
        offers = dict()

        # Модуль смена тарифа
        if pd.notnull(adv_sugg.change_pu_new) and adv_sugg.change_pu_new in TARIFFS:
            tariff_change = {
                'code' : adv_sugg.change_pu_new,
                'profit' : adv_sugg.change_pu_profit,
            }

            offers['tariffChange'] = tariff_change

        if len(offers) > 0:
            actual_suggestions['offers'] = offers

        # Модуль расчета изменения среднерыночной стоимости бизнеса
        if pd.notnull(adv_sugg.same_business_change_value):
            actual_suggestions['sameBusinnessChangeValue'] = adv_sugg.same_business_change_value

        # Модуль причин возможной блокировки
        block_reasons = list()
        for i in range(1,9):
            block_reason = adv_sugg['block_reason_' + str(i)]
            if pd.notnull(block_reason) and block_reason in BLOCKING_REASONS:
                block_reasons.append(block_reason)

        if len(block_reasons) > 0:
            actual_suggestions['blockingReasons'] = block_reasons

        # Модуль возможных причин обращения
        appeal_themes = list()
        for i in range(1, 4):
            theme = appeal_themes['themes_appeal_' + str(i)]
            if pd.notnull(theme) and theme >= 1 and theme <= 33:
                appeal_themes.append(theme)

        if len(appeal_themes) > 0:
            actual_suggestions['information'] = appeal_themes

        if len(actual_suggestions) == 1 and actual_suggestions.get('sameBusinnessChangeValue') is not None:
            actual_suggestions = None

        return actual_suggestions
                  
    def process_user_feedback(self, inn, congr_code, celebr_year, kpp=None):
        res = dict()
        if congr_code not in CONGRATULATION_CODES:
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный тип поздравления'
            return res

        celebr_data = {congr_code: celebr_year}

        if not check_inn(inn):
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'
        else:
            if kpp is not None and not check_kpp(kpp):
                res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный КПП'
            else:
                user_state_df = self.get_user_state(inn, kpp)
                if user_state_df is None:
                    res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
                elif user_state_df.shape[0] == 0:
                    res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствует запись для переданного ИНН'
                elif user_state_df.shape[0] > 1:
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                else:
                    if not self.update_user_state(inn, celebr_data, kpp):
                        res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'

        return res

    def get_predict(self, inn, lrp_fullname, kpp=None):
        res = dict()

        if not check_inn(inn):
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'
        else:
            if kpp is not None and not check_kpp(kpp):
                res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный КПП'
            else:
                adv_sugg_df = self.get_advisor_suggestions(inn, kpp)
                if adv_sugg_df is None:
                    res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
                elif adv_sugg_df.shape[0] == 0: # Предложений Советника не найдено
                    user_state_df = self.get_user_state(inn, kpp)
                    if user_state_df is None:
                        res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
                    elif user_state_df.shape[0] == 0: # В таблице событий запись также отсутствует
                        res['show'] = False
                    elif user_state_df.shape[0] > 1: # В таблице событий больше одной записи по ИНН
                        res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                    else: # В таблице событий присутствует запись, показывать иконку Советника необходимо
                        res['show'] = True
                elif adv_sugg_df.shape[0] > 1: # Найдено более одного предложения по ИНН
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                else:
                    user_state_df = self.get_user_state(inn, kpp)
                    if user_state_df is None:
                        res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
                    elif user_state_df.shape[0] > 1: # В таблице событий больше одной записи по ИНН
                        res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                    else:
                        # Обработка предложений
                        act_sugg = self.actual_suggestions_transform(adv_sugg_df, user_state_df, lrp_fullname)
                        if act_sugg is None:
                            if user_state_df.shape[0] == 0:
                                res['show'] = False
                            else:
                                res['show'] = True
                        else:
                            res.update(act_sugg)
                            res['show'] = True

        return res






                
        if inn != None and lrp_fullname != None and check_inn(inn) and check_kpp(kpp):
            try: 
                advisor_dict = self.get_row_block(inn, kpp, self.advisor_tablename)
                event_dict = self.get_row_block(inn, kpp, self.advisor_events_tablename)
            except:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Ошибка обращения к БД'
                    res['advisor_events_table_name'] = self.advisor_events_tablename
            else:
                if event_dict['is_here']==2 or advisor_dict['is_here']==2: 
                                                     res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                                                     res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Дублирование, None в БД'
                else:
                    if   event_dict['is_here']==0 and advisor_dict['is_here']==0: res['show'] = False
                    elif event_dict['is_here']==1 and advisor_dict['is_here']==0: res['show'] = True
                    elif event_dict['is_here']==0 and advisor_dict['is_here']==1: 
                         
                            
                            try:
                                b = self.generate_res_data(advisor_dict['df_row'], lrp_fullname)
                              
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
                                                   
                        #try:
                            res.update(self.generate_res_data(advisor_dict['df_row'], lrp_fullname, events_row=event_dict['df_row'], event_is_empty=0))
                        #except: 
                        #        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        #        res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Обработка данных витрины советника и табл. сост.'
                        #else: res['show'] = True
                    else:  
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Внутренняя ошибка. Такого не может быть'
                                                                                             
        else:
                 res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                 res[resources.RESPONSE_ERROR_FIELD] = 'Внешняя ошибка. Некорректный ИНН или КПП или ФИО'
    
        return res













