from common import MLModel
from models.revenue_spark.config import REVENUE_SPARK_TABLE
from utils import resources
from utils import check_inn
import pandas as pd
import traceback

class RevenueSpark(MLModel):
    def __init__(self):
        super().__init__()

        self.tablename = REVENUE_SPARK_TABLE

    def get_revenue(self, inn):
        sql_query = f"""
                        select
                            t.ddate,
                            t.inn,
                            sp.kpp,
                            sp.short_name_rus,
                            t.segment
                                from {self.tablename} t
                                join pxu_dcbul_uma_mod.leo_spark_base sp
                                    on t.spark_id = sp.spark_id and sp.inn = :inn 
                                    where 1=1
                                    and t.ddate = (select max(ddate) from {self.tablename})
                    """

        revenue_df = self.read_sql_query(sql_query, params=[inn, ])
        return revenue_df

    def get_predict(self, inn, kpp=None):
        res = dict()
        res['inn'] = inn
        res['kpp'] = kpp
        if check_inn(inn):
            revenue_df = self.get_revenue(inn)
            if revenue_df is None:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            elif revenue_df.shape[0] == 0:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ИНН'
            elif revenue_df.shape[0] > 1:
                if kpp is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                else:
                    revenue_df = revenue_df.loc[revenue_df.kpp == kpp, :]
                    if revenue_df.shape[0] == 0:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданных ИНН и КПП'
                    else:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                        res['name_rus'] = revenue_df.short_name_rus[0]
                        res['segment'] = revenue_df.segment[0]

            else:
                res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                res['name_rus'] = revenue_df.short_name_rus[0]
                res['segment'] = revenue_df.segment[0]
        else:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'

        return res