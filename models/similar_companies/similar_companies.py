from common import MLModel
from utils import check_inn
from utils import resources
import  models.similar_companies.cython_funcs as cython_funcs
from models.similar_companies.config import FULL_MARKET_TABLE, CLIENTS_TABLE
from models.similar_companies.config import ID_COLS, CONT_FEATURES_COLS, ORDER_FEATURES_COLS, CATEG_FEATURES_COLS
from models.similar_companies.config import IMPUTER_FILENAME, TRANSFORMER_FILENAME, NN_MODELS_FILENAME,\
                                            CLIENTS_IDS_FILENAME
import os
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.compose import make_column_transformer, ColumnTransformer
from sklearn.neighbors import NearestNeighbors


class SimilarCompanies(MLModel):
    def __init__(self):
        super().__init__()
        self.full_market_tablename = FULL_MARKET_TABLE
        self.clients_tablename = CLIENTS_TABLE

        # словарь ОКВЭДОВ клиентов, для которых есть обученные модели
        self.okv_groups = [
            '46', '43', '41', '68', '47', '49', '45', '52',
            '69', '71', '56', '73', '94', '81', '86', '62',
            '42', '25', '80', '33', '79', '85', '64', '70',
            '93', '82', '10', '96', '01', '72', '95', '23',
            '77', '38', '16', '22', '63', '31', '35', '28',
            '18', '55', '74', '14', '61', '78', '66', '58',
            '20', '02', '26', '27', '32', '88', '08', '90',
            '13', '53', '59', '84', '11', '50', '03', '60',
            '09', '24', '17', '37', '29', '30', '91', '15',
            '36', '75', '51', '07', '21', '87', '19', '06',
            '65', '39'
        ]

        self.load_metadata()

    def fit(self):
        res = dict()

        full_market = self.read_sql_table(self.full_market_tablename)
        if full_market is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif full_market.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            full_market = self.preprocess_table(full_market)

        # Считаем частотность значений для категориальных признаков и моду для последующей кодировки
        categ_feat_modes = dict()
        categ_feat_transform_dict = dict()
        for feature in CATEG_FEATURES_COLS:
            categ_feat_modes[feature] = full_market.loc[:, feature].mode()[0]
            categ_feat_transform_dict[feature] = full_market.loc[:, feature].value_counts().to_dict()

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        cols_names = CONT_FEATURES_COLS + ORDER_FEATURES_COLS + CATEG_FEATURES_COLS + ID_COLS
        full_market = full_market.loc[:, cols_names]

        # Заполнение пропусков перменных
        categ_imputers = [(SimpleImputer(strategy='constant', fill_value=categ_feat_modes[feat]), [feat, ]) \
                          for feat in CATEG_FEATURES_COLS] # для каждой категориальной переменной свое значение

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.column_imputer = make_column_transformer(
            (SimpleImputer(strategy='median'), CONT_FEATURES_COLS),
            (SimpleImputer(strategy='constant'), ORDER_FEATURES_COLS),
            *categ_imputers,
            remainder='passthrough',
            n_jobs=4
        )

        # Обучаем ColumnTransformer и заполняем пропуски
        full_market = pd.DataFrame(self.column_imputer.fit_transform(full_market), columns=cols_names)

        # Преобразования переменных
        # Кодируем частотой категориальные перменные и нормализуем
        categ_transformer = Pipeline(steps=[
            ('encoder', FunctionTransformer(self.freq_coding, kw_args={'value_cnt_dict': categ_feat_transform_dict})),
            ('scaler', StandardScaler()),
        ])

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.column_tranformer = make_column_transformer(
            (StandardScaler(), CONT_FEATURES_COLS),
            (StandardScaler(), ORDER_FEATURES_COLS),
            (categ_transformer, CATEG_FEATURES_COLS),
            remainder='passthrough',
            n_jobs=None # !!! не ставить другое значение, иначе нельзя сохранить данный класс на диск
        )

        # Обучаем ColumnTransformer для преобразования переменных
        self.column_tranformer.fit(full_market)
        del full_market # освобождаем память

        # Обучение модели

        # Загружаем таблицу с клиентами
        clients = self.read_sql_table(self.clients_tablename)
        if clients is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif clients.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            clients = self.preprocess_table(clients)

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        clients = clients.loc[:, cols_names]

        clients = pd.DataFrame(self.column_imputer.transform(clients), columns=cols_names) # заполняем пропуски
        clients = pd.DataFrame(self.column_tranformer.transform(clients), columns=cols_names) # преобразуем переменные

        # Список переменных модели
        features_cols = CONT_FEATURES_COLS + ORDER_FEATURES_COLS + CATEG_FEATURES_COLS

        self.nnbrs_models = {}
        for okv_group in self.okv_groups:
            train_X = clients.loc[clients.okved_code == okv_group, features_cols]
            nnbrs_model = NearestNeighbors(n_neighbors=9, algorithm='ball_tree', n_jobs=6, metric=cython_funcs.weighted_dist)
            self.nnbrs_models[okv_group] = nnbrs_model.fit(train_X)

        # Устанавливаем индексы для каждой группы оквэдов
        clients['idx_in_okv_gr'] = clients.groupby(['okved_code']).cumcount()

        # Оставляем только идентификаторы клиентов, чтобы по индексу в группе определить клиента
        self.clients_ids = clients.loc[:, ['client_key', 'okved_code', 'idx_in_okv_gr']]

        # Сохраняем обученные данные
        self.save_metadata()

        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
        return res

    def get_company_feats(self, company_id):
        sql_query = f"""
                        select
                            t.*
                                from {self.full_market_tablename} t
                                    where 1=1
                                    and t.client_key = :cid
                    """

        company_feats_df = self.read_sql_query(sql_query, params=[company_id, ])
        return company_feats_df

    def predict(self, inn, kpp=None):
        res = dict()
        res['inn'] = inn
        res['kpp'] = kpp
        if self.column_imputer is None or self.column_tranformer is None or self.nnbrs_models is None \
                or self.clients_ids is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Модель не обучена, предсказания невозможны'
            return res

        if check_inn(inn):
            company_id = inn + '_'
            if kpp is not None:
                company_id += kpp
            company_feats = self.get_company_feats(company_id)
            if company_feats is None:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            elif company_feats.shape[0] == 0:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданных ИНН и КПП'
            else:
                company_feats = self.preprocess_table(company_feats)

                # Упорядочиваем столбцы датасета для дальнейших преобразований
                cols_names = CONT_FEATURES_COLS + ORDER_FEATURES_COLS + CATEG_FEATURES_COLS + ID_COLS
                company_feats = company_feats.loc[:, cols_names]

                company_feats = pd.DataFrame(self.column_imputer.transform(company_feats), columns=cols_names)
                company_feats = pd.DataFrame(self.column_tranformer.transform(company_feats), columns=cols_names)

                features_cols = CONT_FEATURES_COLS + ORDER_FEATURES_COLS + CATEG_FEATURES_COLS

                # Используем обученную модель в зависимости от ОКВЭД
                comp_okved_code = company_feats['okved_code'][0]
                if comp_okved_code in self.nnbrs_models:
                    dists, idxs = self.nnbrs_models[comp_okved_code].kneighbors(company_feats.loc[:, features_cols])
                    # Преобразуем numpy массивы к dataframe
                    nneighbors = pd.DataFrame({'idx_in_okv_gr': idxs[0], 'distanse': dists[0]})

                    # Оставляем только необходимый ОКВЭД
                    clients_ids = self.clients_ids.loc[self.clients_ids.okved_code == comp_okved_code, :]

                    # Получаем информацию о похожих компаниях
                    nneighbors = nneighbors.merge(clients_ids, on='idx_in_okv_gr')
                    nneighbors = nneighbors.loc[:, ['client_key', 'distanse']]
                    nneighbors = nneighbors.sort_values('distanse')

                    res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                    res['similar_clients'] = nneighbors.to_dict(orient='records')
                else:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
        else:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'

        return res

    def preprocess_table(self, df):
        df = df.drop(columns=['ddate', ]) # убираем столбец с датой

        # Приводим числовые признаки к одному типу
        df.loc[:, CONT_FEATURES_COLS] = df.loc[:, CONT_FEATURES_COLS].astype(float)
        df.loc[:, ORDER_FEATURES_COLS] = df.loc[:, ORDER_FEATURES_COLS].astype(float)
        # Преобразуем пропуски к одному виду
        df.loc[:, CATEG_FEATURES_COLS] = df.loc[:, CATEG_FEATURES_COLS].fillna(np.nan)
        df.loc[:, CATEG_FEATURES_COLS] = df.loc[:, CATEG_FEATURES_COLS] \
                                            .convert_dtypes(convert_string=False)
        return df

    @staticmethod
    def freq_coding(df, value_cnt_dict):
        """
        Кодирование dataframe с помощью словаря частот признака
        :param df: pandas dataframe, который кодируем
        :param value_cnt_dict: словарь, содержит словари частот по каждому названию стобцов из df
        :return: преобразованный pandas dataframe
        """
        for feat in value_cnt_dict.keys():
            df.loc[:, feat] = df.loc[:, feat].map(value_cnt_dict[feat])
        return df

    def save_metadata(self):
        new_suffix = '.new'

        fname, ext = os.path.splitext(TRANSFORMER_FILENAME)
        TRANSFORMER_FILENAME_TMP = fname + new_suffix + ext
        fname, ext = os.path.splitext(IMPUTER_FILENAME)
        IMPUTER_FILENAME_TMP = fname + new_suffix + ext
        fname, ext = os.path.splitext(NN_MODELS_FILENAME)
        NN_MODELS_FILENAME_TMP = fname + new_suffix + ext
        fname, ext = os.path.splitext(CLIENTS_IDS_FILENAME)
        CLIENTS_IDS_FILENAME_TMP = fname + new_suffix + ext

        self.save_object_in_joblib(self.column_imputer, TRANSFORMER_FILENAME_TMP)
        self.save_object_in_joblib(self.column_tranformer, IMPUTER_FILENAME_TMP)
        self.save_object_in_joblib(self.nnbrs_models, NN_MODELS_FILENAME_TMP)
        self.save_df_in_pickle(self.clients_ids, CLIENTS_IDS_FILENAME_TMP)

        os.rename(TRANSFORMER_FILENAME_TMP, TRANSFORMER_FILENAME)
        os.rename(IMPUTER_FILENAME_TMP, IMPUTER_FILENAME)
        os.rename(NN_MODELS_FILENAME_TMP, NN_MODELS_FILENAME)
        os.rename(CLIENTS_IDS_FILENAME_TMP, CLIENTS_IDS_FILENAME)

    def load_metadata(self):
        self.column_imputer = self.load_joblib_file(IMPUTER_FILENAME)
        self.column_tranformer = self.load_joblib_file(TRANSFORMER_FILENAME)
        self.nnbrs_models = self.load_joblib_file(NN_MODELS_FILENAME)
        self.clients_ids = self.load_pickle_table(CLIENTS_IDS_FILENAME)

