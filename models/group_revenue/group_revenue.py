from common import MLModel
from models.group_revenue.config import SPARK_ORGANIZATIONS_INFO
from utils import resources
from utils import check_inn
import pandas as pd
import traceback

class GroupRevenue(MLModel):
    orgnz_info_tablename = SPARK_ORGANIZATIONS_INFO

    def __init__(self):
        super().__init__()

    def get_group_revenue(self, inn):
        sql_query = f"""
                        select
                            t.inn,
                            t.kpp,
                            t.short_name_rus,
                            t.main_company,
                            t.sum_revenue_group,
                            t.count_company_in_group
                                from {self.orgnz_info_tablename} t
                                    where 1=1
                                    and t.inn = :inn
                    """

        group_revenue_df = self.read_sql_query(sql_query, params=[inn, ])
        return group_revenue_df

    def get_predict(self, inn, kpp=None):
        res = dict()
        if check_inn(inn):
            group_revenue_df = self.get_group_revenue(inn)
            if group_revenue_df is None:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            elif group_revenue_df.shape[0] == 0:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ИНН'
            elif group_revenue_df.shape[0] > 1:
                if kpp is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                else:
                    group_revenue_df = group_revenue_df.loc[group_revenue_df.kpp == kpp, :]
                    if group_revenue_df.shape[0] == 0:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданных ИНН и КПП'
                    else:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                        res['inn'] = group_revenue_df.inn[0]
                        res['kpp'] = group_revenue_df.kpp[0]
                        res['name_rus'] = group_revenue_df.short_name_rus[0]
                        res['main_company'] = True if group_revenue_df.main_company[0] == 1 else False
                        res['group_size'] = group_revenue_df.count_company_in_group[0]
                        res['sum_revenue_group'] = group_revenue_df.sum_revenue_group[0]

            else:
                res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                res['name_rus'] = group_revenue_df.short_name_rus[0]
                res['segment'] = group_revenue_df.segment[0]
        else:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'

        return res