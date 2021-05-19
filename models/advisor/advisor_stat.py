from models.advisor.config import ADVISOR_TABLE, ADVISOR_EVENTS_TABLE, ADVISOR_USER_BDAY_TABLE
from models.advisor.config import CONGRATULATION_CODES, BLOCKING_REASONS, PRODUCT_OFFERS, TARIFFS
from utils import resources
from common import OracleDB

import datetime
import calendar
import pandas as pd
from sqlalchemy import INTEGER, FLOAT, VARCHAR, DATE


class AdvisorStat(OracleDB):
    advisor_tablename = ADVISOR_TABLE
    advisor_events_tablename = ADVISOR_EVENTS_TABLE
    advisor_user_bday_tablename = ADVISOR_USER_BDAY_TABLE

    user_state_dtype = {
        'organization_id': VARCHAR(200),
        'person_id': VARCHAR(200),
        'birthday': INTEGER,
        'company_anniversary': INTEGER,
        'client_anniversary': INTEGER,
        'industry_day': INTEGER,
    }

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
            prevday = today.replace(day=min(today.day, calendar.monthrange(hday.year - 1, today.month)[1]),
                                    year=hday.year - 1)
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
            nextday = today.replace(day=min(today.day, calendar.monthrange(hday.year + 1, today.month)[1]),
                                    year=hday.year + 1)
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

    def get_advisor_suggestions(self, organization_id, person_id):
        sql_query = f"""
                        with bday_holidays as (
                            select
                            t.organization_id,
                            t.person_id,
                            t.birth_date
                                from {self.advisor_user_bday_tablename} t
                                    where 1=1
                                    and t.person_id = :pers_id
                                    and t.organization_id = :org_id
                                    and t.date_upload = (select max(date_upload) from {self.advisor_user_bday_tablename})
                        ),
                        
                        advisor_suggs as (
                            select
                            t.organization_id,
                            t.industry_name, t.industry_day,
                            t.company_anniversary, t.company_count_year,
                            t.client_anniversary, t.client_count_year,
                            t.block_reason_1, t.block_reason_2, t.block_reason_3, t.block_reason_4,
                            t.block_reason_5, t.block_reason_6, t.block_reason_7, t.block_reason_8,
                            t.same_business_change_value,
                            t.salary_project, t.business_cards, t.credit, t.deposit, t.acquiring, t.guarantee, t.nso,
                            t.ved, t.sms, t.encashment, t.mobile_banking,
                            t.change_pu_new, t.change_pu_profit,
                            t.themes_appeal_1, t.themes_appeal_2, t.themes_appeal_3
                                from {self.advisor_tablename} t
                                        where 1=1
                                        and t.organization_id = :org_id
                                        and t.ddate = (select max(ddate) from {self.advisor_tablename})
                        )
                        
                        select
                        b.person_id,
                        b.birth_date,
                        a.*
                            from bday_holidays b
                            full join advisor_suggs a on b.organization_id = a.organization_id                     
                    """

        adv_sugg_df = self.read_sql_query(sql_query,
                                          params={
                                              'org_id': organization_id,
                                              'pers_id': person_id,
                                          })
        return adv_sugg_df

    def get_user_state(self, organization_id, person_id):
        sql_query = f"""
                        select
                            t.birthday,
                            t.company_anniversary,
                            t.client_anniversary,
                            t.industry_day
                                from {self.advisor_events_tablename} t
                                    where 1=1
                                    and t.organization_id = :org_id
                                    and t.person_id = :pers_id
                    """

        user_state_df = self.read_sql_query(sql_query,
                                            params={
                                                'org_id': organization_id,
                                                'pers_id': person_id,
                                            })
        return user_state_df

    def update_user_state(self, prev_user_state_df, organization_id, person_id, data):
        if prev_user_state_df.shape[0] == 0:
            for key in data.keys():
                prev_user_state_df.loc[0, key.lower()] = data[key]

            prev_user_state_df.loc[0, 'organization_id'] = organization_id
            prev_user_state_df.loc[0, 'person_id'] = person_id

            update_success = self.save_df_in_sql_table(prev_user_state_df, self.user_state_dtype,
                                                       self.advisor_events_tablename)
        else:
            sql_query = """
                            update {tablename}
                                set {values_clause}
                                    where 1=1
                                    and organization_id = :org_id
                                    and person_id = :pers_id
                                """

            values_clause = ', '.join([k + ' = :' + k for k in data.keys()])
            params = data
            params['org_id'] = organization_id
            params['pers_id'] = person_id
            sql_query = sql_query.format(tablename=self.advisor_events_tablename, values_clause=values_clause)
            update_success = self.execute_sql_query(sql_query, params=params)

        return update_success

    def actual_suggestions_transform(self, adv_sugg_df, user_state_df, module_name=None):
        adv_sugg = adv_sugg_df.iloc[0]
        user_state = user_state_df.iloc[0].to_dict() if user_state_df.shape[0] == 1 else dict()

        actual_suggestions = dict()

        # Модуль поздравлений
        if module_name is None or module_name == 'congratulations':
            congratulations = list()
            # День рождения ЛПР
            if pd.notnull(adv_sugg.person_id):
                days2hday, clbr_year = self.days_to_holiday(adv_sugg.birth_date)
                prev_clbr_year = user_state.get('birthday')
                if clbr_year != prev_clbr_year and -3 <= days2hday <= 0:  # В течение 3х дней после ДР
                    congratulations.append({
                        'code': 'BIRTHDAY',
                        'date': adv_sugg.birth_date.strftime('%Y-%m-%d'),
                        'celebrationYear': clbr_year,
                    })

            # День регистрации бизнеса
            if pd.notnull(adv_sugg.company_anniversary):
                days2hday, clbr_year = self.days_to_holiday(adv_sugg.company_anniversary)
                prev_clbr_year = user_state.get('company_anniversary')
                if clbr_year != prev_clbr_year and -5 <= days2hday <= 5:
                    congratulations.append({
                        'code': 'COMPANY_ANNIVERSARY',
                        'date': adv_sugg.company_anniversary.strftime('%Y-%m-%d'),
                        'celebrationYear': clbr_year,
                        'fullYearsFromDate': int(adv_sugg.company_count_year),
                    })

            # День отрасли
            if pd.notnull(adv_sugg.industry_name):
                days2hday, clbr_year = self.days_to_holiday(adv_sugg.industry_day)
                prev_clbr_year = user_state.get('industry_day')
                if clbr_year != prev_clbr_year and -5 <= days2hday <= 5:
                    congratulations.append({
                        'code': 'INDUSTRY_DAY',
                        'indName': adv_sugg.industry_name,
                        'date': adv_sugg.industry_day.strftime('%Y-%m-%d'),
                        'celebrationYear': clbr_year,
                    })

            # День начала обслуживания в Банке
            if pd.notnull(adv_sugg.client_anniversary):
                days2hday, clbr_year = self.days_to_holiday(adv_sugg.client_anniversary)
                prev_clbr_year = user_state.get('client_anniversary')
                if clbr_year != prev_clbr_year and -5 <= days2hday <= 5:
                    congratulations.append({
                        'code': 'CLIENT_ANNIVERSARY',
                        'date': adv_sugg.client_anniversary.strftime('%Y-%m-%d'),
                        'celebrationYear': clbr_year,
                        'fullYearsFromDate': int(adv_sugg.client_count_year),
                    })

            if len(congratulations) > 0:
                actual_suggestions['congratulations'] = congratulations[0]

        # Модуль предложений по продуктам
        if module_name is None or module_name == 'productsOffer':
            products_offers = list()
            for product_name in PRODUCT_OFFERS:
                if adv_sugg[product_name.lower()] == 1:
                    products_offers.append(product_name)

            if len(products_offers) > 0:
                actual_suggestions['productsOffer'] = products_offers

        # Модуль выгодных предложений
        if module_name is None or module_name == 'offers':
            offers = dict()

            # Модуль смена тарифа
            if pd.notnull(adv_sugg.change_pu_new) and self.check_tariff_name(adv_sugg.change_pu_new):
                tariff_change = {
                    'code': adv_sugg.change_pu_new,
                    'profit': adv_sugg.change_pu_profit,
                }

                offers['tariffChange'] = tariff_change

            if len(offers) > 0:
                actual_suggestions['offers'] = offers

        # Модуль расчета изменения среднерыночной стоимости бизнеса
        if module_name is None or module_name == 'businessEvaluation':
            business_values = dict()

            if pd.notnull(adv_sugg.same_business_change_value):
                business_values['sameBusinessChangeValue'] = adv_sugg.same_business_change_value

            if len(business_values) > 0:
                actual_suggestions['businessEvaluation'] = business_values

        # Модуль причин возможной блокировки
        if module_name is None or module_name == 'blockingReasons':
            block_reasons = list()
            for i in range(1, 9):
                block_reason = adv_sugg['block_reason_' + str(i)]
                if pd.notnull(block_reason) and self.check_block_reason(block_reason):
                    block_reasons.append(block_reason)

            if len(block_reasons) > 0:
                actual_suggestions['blockingReasons'] = block_reasons

        # Модуль возможных причин обращения
        if module_name is None or module_name == 'information':
            appeal_themes = list()
            for i in range(1, 4):
                theme = adv_sugg['themes_appeal_' + str(i)]
                if pd.notnull(theme) and 1 <= theme <= 33:
                    appeal_themes.append(int(theme))

            if len(appeal_themes) > 0:
                actual_suggestions['information'] = appeal_themes

        if len(actual_suggestions) == 0 \
                or (module_name is None and len(actual_suggestions) == 1
                    and actual_suggestions.get('businessEvaluation') is not None):
            actual_suggestions = None

        return actual_suggestions

    def process_user_feedback(self, organization_id, person_id, congr_code, celebr_year):
        res = dict()
        if self.check_congr_code(congr_code):
            celebr_data = {congr_code: celebr_year}

            user_state_df = self.get_user_state(organization_id, person_id)
            if user_state_df is None:
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            else:
                if not self.update_user_state(user_state_df, organization_id, person_id, celebr_data):
                    res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
        else:
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный тип поздравления'

        return res

    def check_actual_suggestions(self, organization_id, person_id):
        res = dict()

        adv_sugg_df = self.get_advisor_suggestions(organization_id, person_id)
        if adv_sugg_df is None:
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
        elif adv_sugg_df.shape[0] == 1:  # Найдены предложения Советника
            user_state_df = self.get_user_state(organization_id, person_id)
            if user_state_df is None:
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            else:  # Проверка актуальных предложений
                act_sugg = self.actual_suggestions_transform(adv_sugg_df, user_state_df)
                if act_sugg is not None:
                    res['actualSuggestions'] = True
                else:
                    res['actualSuggestions'] = False
        else:
            res['actualSuggestions'] = False
        return res

    def get_predict(self, organization_id, person_id, module_name=None):
        res = dict()

        if module_name == 'all':
            module_name = None

        adv_sugg_df = self.get_advisor_suggestions(organization_id, person_id)
        if adv_sugg_df is None:
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
        elif adv_sugg_df.shape[0] == 1:  # Найдены предложения Советника
            user_state_df = self.get_user_state(organization_id, person_id)
            if user_state_df is None:
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            else:  # Обработка предложений
                act_sugg = self.actual_suggestions_transform(adv_sugg_df, user_state_df, module_name)
                if act_sugg is not None:
                    res.update(act_sugg)

        return res
