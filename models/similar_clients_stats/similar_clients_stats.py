from common import OracleDB
from utils import resources, check_inn
from models.similar_clients_stats.config import CLIENTS_PORTFOLIO_TABLE
from models.similar_clients_stats.config import SUM_AGGR_COLS, SUM_NOTNULL_AGGR_COLS, AVG_AGGR_COLS, MODA_AGGR_COLS
from models.similar_clients_stats.config import DISTR_AGGR_COLS, DISTR_NULL_AGGR_COLS, TOP_AGGR_COLS
from models.similar_clients_stats.config import ROUND_0_COLS, ROUND_1_COLS, ROUND_2_COLS
from models.similar_clients_stats.config import COLUMN_VALUES_DICT

import pandas as pd
import numpy as np

class SimilarClientsStats(OracleDB):
    col_values_tranl_map = COLUMN_VALUES_DICT
    clients_portfolio_tablename = CLIENTS_PORTFOLIO_TABLE

    def __init__(self):
        super().__init__()

    def get_clients_portf(self, clients_keys):
        bind_names = [':' + str(i) for i in range(9)]

        sql_query = "select * " \
                        "from {tablename} t " \
                            "where t.client_key in ({cl_keys})"
        sql_query = sql_query.format(tablename=self.clients_portfolio_tablename, cl_keys=', '.join(bind_names))
        clients_portf = self.read_sql_query(sql_query, params=clients_keys)
        return clients_portf

    def distribution_aggr(self, neighbours_portf, aggr_cols):
        distr_aggr = pd.Series(dtype='float64')
        for col in aggr_cols:
            transl_dict = self.col_values_tranl_map[col]
            series = neighbours_portf.loc[:, col].map(transl_dict)

            series_distr_aggr = series.value_counts()
            all_values = set(transl_dict.values())
            series_distr_aggr = series_distr_aggr.reindex(all_values, fill_value=0.0)
            series_distr_aggr.index = series_distr_aggr.name + '_' + series_distr_aggr.index

            distr_aggr = distr_aggr.append(series_distr_aggr)
        distr_aggr = distr_aggr.to_frame().transpose()
        return distr_aggr

    def top_aggr(self, neighbours_portf, aggr_cols, top_cnt=3):
        top_aggr = pd.Series(dtype='object')
        for col in aggr_cols:
            series = neighbours_portf.loc[:, col].value_counts()
            series_top_aggr = pd.Series(series.index[:top_cnt])

            top = np.arange(1, top_cnt + 1)
            series_top_aggr.index = series_top_aggr.index + 1
            series_top_aggr = series_top_aggr.reindex(top)
            series_top_aggr.index = series.name + '_' + series_top_aggr.index.astype(str)

            top_aggr = top_aggr.append(series_top_aggr)
        top_aggr = top_aggr.to_frame().transpose()
        return top_aggr

    def similar_clients_aggr(self, similar_clients):
        res = dict()

        similar_clients = pd.DataFrame(similar_clients)
        clients_keys = list(similar_clients.client_key)
        neighbours_portf = self.get_clients_portf(clients_keys)
        if neighbours_portf is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
        else:
            # преобразование числовых переменных
            neighbours_portf.loc[:, SUM_AGGR_COLS] = neighbours_portf.loc[:, SUM_AGGR_COLS].astype(float)
            neighbours_portf.loc[:, SUM_NOTNULL_AGGR_COLS] = neighbours_portf.loc[:, SUM_NOTNULL_AGGR_COLS].applymap(
                                                                            lambda x: 0 if pd.isnull(x) else 1)
            neighbours_portf.loc[:, AVG_AGGR_COLS] = neighbours_portf.loc[:, AVG_AGGR_COLS].astype(float)

            # заполнение пропусков и преобразование категориальных переменных
            cols_categ = MODA_AGGR_COLS + DISTR_AGGR_COLS + DISTR_NULL_AGGR_COLS + TOP_AGGR_COLS
            neighbours_portf.loc[:, cols_categ] = neighbours_portf.loc[:, cols_categ].fillna(np.nan)
            neighbours_portf.loc[:, cols_categ] = neighbours_portf.loc[:, cols_categ]\
                                                                                .convert_dtypes(convert_string=False)

            neighbours_portf.loc[:, DISTR_NULL_AGGR_COLS] = neighbours_portf.loc[:, DISTR_NULL_AGGR_COLS]\
                                                                                            .fillna(value='Unknown')

            # агрегация данных
            sum_aggr_portf = neighbours_portf.loc[:, SUM_AGGR_COLS].sum().to_frame().transpose()
            sum_notnull_aggr_portf = neighbours_portf.loc[:, SUM_NOTNULL_AGGR_COLS].sum().to_frame().transpose()
            avg_aggr_portf = neighbours_portf.loc[:, AVG_AGGR_COLS].mean().to_frame().transpose()
            moda_aggr_portf = neighbours_portf.loc[:, MODA_AGGR_COLS].mode().head(1)
            distr_aggr_portf = self.distribution_aggr(neighbours_portf, DISTR_AGGR_COLS)
            distr_null_aggr_portf = self.distribution_aggr(neighbours_portf, DISTR_NULL_AGGR_COLS)
            top_aggr_portf = self.top_aggr(neighbours_portf, TOP_AGGR_COLS)

            # округление значений
            avg_aggr_portf.loc[:, ROUND_0_COLS] = avg_aggr_portf.loc[:, ROUND_0_COLS].round(0)
            avg_aggr_portf.loc[:, ROUND_1_COLS] = avg_aggr_portf.loc[:, ROUND_1_COLS].round(1)
            avg_aggr_portf.loc[:, ROUND_2_COLS] = avg_aggr_portf.loc[:, ROUND_2_COLS].round(2)

            neighbours_aggr = pd.concat([sum_aggr_portf, sum_notnull_aggr_portf, avg_aggr_portf, moda_aggr_portf,
                                                    distr_aggr_portf, distr_null_aggr_portf, top_aggr_portf], axis=1)
            res['neighbours_stats'] = neighbours_aggr.to_dict(orient='records')
            res[resources.RESPONSE_STATUS_FIELD] = 'Ok'

        return res
    