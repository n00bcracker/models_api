from common import MLModel
from models.similar_clients.config import FULL_MARKET_IE_TABLE, FULL_MARKET_COMP_TABLE
from models.similar_clients.config import CLIENTS_IE_TABLE, CLIENTS_COMP_TABLE

from models.similar_clients.config import IE_ID_COLS, COMP_ID_COLS
from models.similar_clients.config import IE_CONT_FEATURES_COLS, COMP_CONT_FEATURES_COLS
from models.similar_clients.config import IE_ORDER_FEATURES_COLS, COMP_ORDER_FEATURES_COLS
from models.similar_clients.config import IE_BIN_CATEG_COLS, COMP_BIN_CATEG_COLS
from models.similar_clients.config import IE_CATEG_FEATURES_COLS, COMP_CATEG_FEATURES_COLS
from models.similar_clients.config import IE_IMPUTER_FILENAME, COMP_IMPUTER_FILENAME
from models.similar_clients.config import IE_TRANSFORMER_FILENAME, COMP_TRANSFORMER_FILENAME
from models.similar_clients.config import IE_NN_MODELS_FILENAME, COMP_NN_MODELS_FILENAME
from models.similar_clients.config import IE_CLIENTS_IDS_FILENAME, COMP_CLIENTS_IDS_FILENAME
from models.similar_clients.config import SIMCOMP_METADIR
from utils import resources, check_inn
import models.similar_clients.cython_funcs as cython_funcs

import os
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OrdinalEncoder, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.compose import make_column_transformer
from sklearn.neighbors import NearestNeighbors


class SimilarClients(MLModel):
    full_market_ie_tablename = FULL_MARKET_IE_TABLE
    full_market_comp_tablename = FULL_MARKET_COMP_TABLE
    clients_ie_tablename = CLIENTS_IE_TABLE
    clients_comp_tablename = CLIENTS_COMP_TABLE

    # список ОКВЭДов клиентов, для которых есть обученные модели
    ie_okv_groups = [
        '47', '49', '43', '46', '68', '45', '41', '69', '56', '96', '73', '62', '95', '81', '52', '70',
        '01', '77', '74', '82', '71', '93', '25', '31', '33', '10', '85', '14', '16', '66', '79', '63',
        '18', '90', '55', '42', '38', '23', '13', '02', '59', '32', '86', '22', '88', '53', '58', '78',
        '72', '80', '61', '75', '64', '28', '03', '65', '35', '15', '26', '20', '17', '27', '50', '60',
        '91', '37',
    ]

    comp_okv_groups = [
        '46', '43', '41', '68', '47', '49', '45', '52', '69', '71', '56', '73', '94', '81', '86', '62',
        '42', '25', '80', '33', '79', '85', '64', '70', '93', '82', '10', '96', '01', '72', '95', '23',
        '77', '38', '16', '22', '63', '31', '35', '28', '18', '55', '74', '14', '61', '78', '66', '58',
        '20', '02', '26', '27', '32', '88', '08', '90', '13', '53', '59', '84', '11', '50', '03', '60',
        '09', '24', '17', '37', '29', '30', '91', '15', '36', '75', '51', '07', '21', '87', '19', '06',
        '65', '39',
    ]

    def __init__(self):
        super().__init__()

        if not os.path.isdir(SIMCOMP_METADIR):
            os.mkdir(SIMCOMP_METADIR)

        self.load_metadata()

    def preprocess_ie_table(self, df):
        df = df.drop(columns=['ddate', ]) #  убираем столбец с датой

        # Приводим числовые признаки к одному типу
        df.loc[:, IE_CONT_FEATURES_COLS] = df.loc[:, IE_CONT_FEATURES_COLS].astype(float)
        df.loc[:, IE_ORDER_FEATURES_COLS] = df.loc[:, IE_ORDER_FEATURES_COLS].astype(float)
        # Преобразуем пропуски к одному виду
        df.loc[:, IE_CATEG_FEATURES_COLS] = df.loc[:, IE_CATEG_FEATURES_COLS].fillna(np.nan)
        df.loc[:, IE_CATEG_FEATURES_COLS] = df.loc[:, IE_CATEG_FEATURES_COLS].convert_dtypes(convert_string=False)
        return df

    def preprocess_comp_table(self, df):
        df = df.drop(columns=['ddate', ]) #  убираем столбец с датой

        # Приводим числовые признаки к одному типу
        df.loc[:, COMP_CONT_FEATURES_COLS] = df.loc[:, COMP_CONT_FEATURES_COLS].astype(float)
        df.loc[:, COMP_ORDER_FEATURES_COLS] = df.loc[:, COMP_ORDER_FEATURES_COLS].astype(float)
        # Преобразуем пропуски к одному виду
        df.loc[:, COMP_CATEG_FEATURES_COLS] = df.loc[:, COMP_CATEG_FEATURES_COLS].fillna(np.nan)
        df.loc[:, COMP_CATEG_FEATURES_COLS] = df.loc[:, COMP_CATEG_FEATURES_COLS].convert_dtypes(convert_string=False)
        return df

    def fit(self):
        res = dict()

        # Преобразование таблицы по ИП
        full_market_ie = self.read_sql_table(self.full_market_ie_tablename)
        if full_market_ie is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif full_market_ie.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            full_market_ie = self.preprocess_ie_table(full_market_ie)

        # Считаем частотность значений для категориальных признаков и моду для последующей кодировки
        ie_categ_feat_modes = dict()
        ie_categ_feat_transform_dict = dict()
        for feature in IE_CATEG_FEATURES_COLS:
            ie_categ_feat_modes[feature] = full_market_ie.loc[:, feature].mode()[0]
            ie_categ_feat_transform_dict[feature] = full_market_ie.loc[:, feature].value_counts().to_dict()

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        ie_cols_names = IE_CONT_FEATURES_COLS + IE_ORDER_FEATURES_COLS + IE_CATEG_FEATURES_COLS + IE_BIN_CATEG_COLS\
                                                                                                        + IE_ID_COLS
        full_market_ie = full_market_ie.loc[:, ie_cols_names]

        # Заполнение пропусков перменных
        ie_categ_imputers = [(SimpleImputer(strategy='constant', fill_value=ie_categ_feat_modes[feat]), [feat, ]) \
                            for feat in IE_CATEG_FEATURES_COLS]  # для каждой категориальной переменной свое значение

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.ie_column_imputer = make_column_transformer(
            (SimpleImputer(strategy='median'), IE_CONT_FEATURES_COLS),
            (SimpleImputer(strategy='constant'), IE_ORDER_FEATURES_COLS),
            *ie_categ_imputers,
            remainder='passthrough',
            n_jobs=4
        )

        # Обучаем ColumnTransformer и заполняем пропуски
        full_market_ie = pd.DataFrame(self.ie_column_imputer.fit_transform(full_market_ie), columns=ie_cols_names)

        # Преобразования переменных
        # Кодируем частотой категориальные перменные и нормализуем
        ie_categ_transformer = Pipeline(steps=[
            ('encoder',
                FunctionTransformer(self.freq_coding, kw_args={'value_cnt_dict': ie_categ_feat_transform_dict})),
            ('scaler', StandardScaler()),
        ])

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.ie_column_tranformer = make_column_transformer(
            (StandardScaler(), IE_CONT_FEATURES_COLS),
            (StandardScaler(), IE_ORDER_FEATURES_COLS),
            (ie_categ_transformer, IE_CATEG_FEATURES_COLS),
            (OrdinalEncoder(), IE_BIN_CATEG_COLS),
            remainder='passthrough',
            n_jobs=None  # !!! не ставить другое значение, иначе нельзя сохранить данный класс на диск
        )

        # Обучаем ColumnTransformer для преобразования переменных
        self.ie_column_tranformer.fit(full_market_ie)
        del full_market_ie  # освобождаем память

        # Обучение модели по ИП
        # Загружаем таблицу с клиентами
        ie_clients = self.read_sql_table(self.clients_ie_tablename)
        if ie_clients is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif ie_clients.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            ie_clients = self.preprocess_ie_table(ie_clients)

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        ie_clients = ie_clients.loc[:, ie_cols_names]

        # Заполняем пропуски и преобразуем переменные
        ie_clients = pd.DataFrame(self.ie_column_imputer.transform(ie_clients), columns=ie_cols_names)
        ie_clients = pd.DataFrame(self.ie_column_tranformer.transform(ie_clients), columns=ie_cols_names)

        # Список переменных модели
        ie_features_cols = IE_CONT_FEATURES_COLS + IE_ORDER_FEATURES_COLS + IE_CATEG_FEATURES_COLS\
                                + IE_BIN_CATEG_COLS

        self.ie_nnbrs_models = {}
        for okv_group in self.ie_okv_groups:
            ie_train_X = ie_clients.loc[ie_clients.okved_code == okv_group, ie_features_cols]
            ie_nnbrs_model = NearestNeighbors(n_neighbors=9, algorithm='ball_tree', n_jobs=6,
                                                                            metric=cython_funcs.ie_weighted_dist)
            self.ie_nnbrs_models[okv_group] = ie_nnbrs_model.fit(ie_train_X)

        # Устанавливаем индексы для каждой группы оквэдов
        ie_clients['idx_in_okv_gr'] = ie_clients.groupby(['okved_code']).cumcount()

        # Оставляем только идентификаторы клиентов, чтобы по индексу в группе определить клиента
        self.ie_clients_ids = ie_clients.loc[:, ['client_key', 'okved_code', 'idx_in_okv_gr']]

        del ie_clients

        # Преобразование таблицы по ООО
        full_market_comp = self.read_sql_table(self.full_market_comp_tablename)
        if full_market_comp is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif full_market_comp.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            full_market_comp = self.preprocess_comp_table(full_market_comp)

        # Считаем частотность значений для категориальных признаков и моду для последующей кодировки
        comp_categ_feat_modes = dict()
        comp_categ_feat_transform_dict = dict()
        for feature in COMP_CATEG_FEATURES_COLS:
            comp_categ_feat_modes[feature] = full_market_comp.loc[:, feature].mode()[0]
            comp_categ_feat_transform_dict[feature] = full_market_comp.loc[:, feature].value_counts().to_dict()

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        comp_cols_names = COMP_CONT_FEATURES_COLS + COMP_ORDER_FEATURES_COLS + COMP_CATEG_FEATURES_COLS + COMP_ID_COLS
        full_market_comp = full_market_comp.loc[:, comp_cols_names]

        # Заполнение пропусков перменных
        comp_categ_imputers = [(SimpleImputer(strategy='constant', fill_value=comp_categ_feat_modes[feat]), [feat, ]) \
                          for feat in COMP_CATEG_FEATURES_COLS] # для каждой категориальной переменной свое значение

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.comp_column_imputer = make_column_transformer(
            (SimpleImputer(strategy='median'), COMP_CONT_FEATURES_COLS),
            (SimpleImputer(strategy='constant'), COMP_ORDER_FEATURES_COLS),
            *comp_categ_imputers,
            remainder='passthrough',
            n_jobs=4
        )

        # Обучаем ColumnTransformer и заполняем пропуски
        full_market_comp = pd.DataFrame(self.comp_column_imputer.fit_transform(full_market_comp), columns=comp_cols_names)

        # Преобразования переменных
        # Кодируем частотой категориальные перменные и нормализуем
        comp_categ_transformer = Pipeline(steps=[
            ('encoder', FunctionTransformer(self.freq_coding, kw_args={'value_cnt_dict': comp_categ_feat_transform_dict})),
            ('scaler', StandardScaler()),
        ])

        # Создаем ColumnTransformer сразу для заполнения всех столбцов таблицы
        self.comp_column_tranformer = make_column_transformer(
            (StandardScaler(), COMP_CONT_FEATURES_COLS),
            (StandardScaler(), COMP_ORDER_FEATURES_COLS),
            (comp_categ_transformer, COMP_CATEG_FEATURES_COLS),
            remainder='passthrough',
            n_jobs=None # !!! не ставить другое значение, иначе нельзя сохранить данный класс на диск
        )

        # Обучаем ColumnTransformer для преобразования переменных
        self.comp_column_tranformer.fit(full_market_comp)
        del full_market_comp # освобождаем память

        # Обучение модели
        # Загружаем таблицу с клиентами
        comp_clients = self.read_sql_table(self.clients_comp_tablename)
        if comp_clients is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif comp_clients.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            comp_clients = self.preprocess_comp_table(comp_clients)

        # Упорядочиваем столбцы датасета для дальнейших преобразований
        comp_clients = comp_clients.loc[:, comp_cols_names]

        # Заполняем пропуски и преобразуем переменные
        comp_clients = pd.DataFrame(self.comp_column_imputer.transform(comp_clients), columns=comp_cols_names)
        comp_clients = pd.DataFrame(self.comp_column_tranformer.transform(comp_clients), columns=comp_cols_names)

        # Список переменных модели
        comp_features_cols = COMP_CONT_FEATURES_COLS + COMP_ORDER_FEATURES_COLS + COMP_CATEG_FEATURES_COLS

        self.comp_nnbrs_models = {}
        for okv_group in self.comp_okv_groups:
            comp_train_X = comp_clients.loc[comp_clients.okved_code == okv_group, comp_features_cols]
            comp_nnbrs_model = NearestNeighbors(n_neighbors=9, algorithm='ball_tree', n_jobs=6,
                                                                                metric=cython_funcs.comp_weighted_dist)
            self.comp_nnbrs_models[okv_group] = comp_nnbrs_model.fit(comp_train_X)

        # Устанавливаем индексы для каждой группы оквэдов
        comp_clients['idx_in_okv_gr'] = comp_clients.groupby(['okved_code']).cumcount()

        # Оставляем только идентификаторы клиентов, чтобы по индексу в группе определить клиента
        self.comp_clients_ids = comp_clients.loc[:, ['client_key', 'okved_code', 'idx_in_okv_gr']]

        del comp_clients

        # Сохраняем обученные данные
        self.save_metadata()

        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
        return res

    def get_feats(self, inn):
        sql_query = """
                        select
                            t.*
                                from {tablename} t
                                    where 1=1
                                    and t.inn = :inn
                    """

        if len(inn) == 10:
            sql_query = sql_query.format(tablename=self.full_market_comp_tablename)
        else:
            sql_query = sql_query.format(tablename = self.full_market_ie_tablename)

        feats_df = self.read_sql_query(sql_query, params=[inn, ])
        return feats_df

    def predict(self, inn, kpp=None):
        res = dict()
        res['inn'] = inn
        res['kpp'] = kpp

        if not check_inn(inn):
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'
            return res
        else:
            feats = self.get_feats(inn)
            if feats is None:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
                return res
            elif feats.shape[0] == 0:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ИНН'
                return res
            elif feats.shape[0] > 1:
                if kpp is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                    return res
                else:
                    feats = feats.loc[feats.kpp == kpp, :]
                    if feats.shape[0] == 0:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданных ИНН и КПП'
                        return res

            res['inn'] = feats.inn[0]
            res['kpp'] = feats.iloc[0, :].get('kpp')
            if len(inn) == 10:
                if self.comp_column_imputer is None or self.comp_column_tranformer is None\
                                                or self.comp_nnbrs_models is None or self.comp_clients_ids is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Модель не обучена, предсказания невозможны'
                    return res

                comp_feats = self.preprocess_comp_table(feats)

                # Упорядочиваем столбцы датасета для дальнейших преобразований
                comp_cols_names = COMP_CONT_FEATURES_COLS + COMP_ORDER_FEATURES_COLS + COMP_CATEG_FEATURES_COLS\
                                        + COMP_ID_COLS
                comp_feats = comp_feats.loc[:, comp_cols_names]

                comp_feats = pd.DataFrame(self.comp_column_imputer.transform(comp_feats), columns=comp_cols_names)
                comp_feats = pd.DataFrame(self.comp_column_tranformer.transform(comp_feats), columns=comp_cols_names)

                comp_features_cols = COMP_CONT_FEATURES_COLS + COMP_ORDER_FEATURES_COLS + COMP_CATEG_FEATURES_COLS
                # Используем обученную модель в зависимости от ОКВЭД
                comp_okved_code = comp_feats['okved_code'][0]
                if comp_okved_code in self.comp_nnbrs_models:
                    dists, idxs = self.comp_nnbrs_models[comp_okved_code]\
                                                                    .kneighbors(comp_feats.loc[:, comp_features_cols])
                    # Преобразуем numpy массивы к dataframe
                    comp_nneighbors = pd.DataFrame({'idx_in_okv_gr': idxs[0], 'distanse': dists[0]})

                    # Оставляем только необходимый ОКВЭД
                    comp_clients_ids = self.comp_clients_ids.loc[self.comp_clients_ids.okved_code == comp_okved_code, :]

                    # Получаем информацию о похожих компаниях
                    comp_nneighbors = comp_nneighbors.merge(comp_clients_ids, on='idx_in_okv_gr')
                    comp_nneighbors = comp_nneighbors.loc[:, ['client_key', 'distanse']]
                    comp_nneighbors = comp_nneighbors.sort_values('distanse')

                    res['similar_clients'] = comp_nneighbors.to_dict(orient='records')
                    res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                    return res
                else:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Очень редкий ОКВЭД у компании, похожие компании отсутствуют'
                    return res
            else:
                if self.ie_column_imputer is None or self.ie_column_tranformer is None\
                                                or self.ie_nnbrs_models is None or self.ie_clients_ids is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Модель не обучена, предсказания невозможны'
                    return res

                ie_feats = self.preprocess_ie_table(feats)

                # Упорядочиваем столбцы датасета для дальнейших преобразований
                ie_cols_names = IE_CONT_FEATURES_COLS + IE_ORDER_FEATURES_COLS + IE_CATEG_FEATURES_COLS \
                                  + IE_BIN_CATEG_COLS + IE_ID_COLS
                ie_feats = ie_feats.loc[:, ie_cols_names]

                ie_feats = pd.DataFrame(self.ie_column_imputer.transform(ie_feats), columns=ie_cols_names)
                ie_feats = pd.DataFrame(self.ie_column_tranformer.transform(ie_feats), columns=ie_cols_names)

                ie_features_cols = IE_CONT_FEATURES_COLS + IE_ORDER_FEATURES_COLS + IE_CATEG_FEATURES_COLS \
                                                                                + IE_BIN_CATEG_COLS
                # Используем обученную модель в зависимости от ОКВЭД
                ie_okved_code = ie_feats['okved_code'][0]
                if ie_okved_code in self.ie_nnbrs_models:
                    dists, idxs = self.ie_nnbrs_models[ie_okved_code] \
                                                                .kneighbors(ie_feats.loc[:, ie_features_cols])
                    # Преобразуем numpy массивы к dataframe
                    ie_nneighbors = pd.DataFrame({'idx_in_okv_gr': idxs[0], 'distanse': dists[0]})

                    # Оставляем только необходимый ОКВЭД
                    ie_clients_ids = self.ie_clients_ids.loc[self.ie_clients_ids.okved_code == ie_okved_code, :]

                    # Получаем информацию о похожих компаниях
                    ie_nneighbors = ie_nneighbors.merge(ie_clients_ids, on='idx_in_okv_gr')
                    ie_nneighbors = ie_nneighbors.loc[:, ['client_key', 'distanse']]
                    ie_nneighbors = ie_nneighbors.sort_values('distanse')

                    res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                    res['similar_clients'] = ie_nneighbors.to_dict(orient='records')
                    return res
                else:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Очень редкий ОКВЭД у ИП, похожие ИП отсутствуют'
                    return res

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

        fname, ext = os.path.splitext(IE_TRANSFORMER_FILENAME)
        ie_transformer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(IE_IMPUTER_FILENAME)
        ie_imputer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(IE_NN_MODELS_FILENAME)
        ie_nn_models_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(IE_CLIENTS_IDS_FILENAME)
        ie_clients_ids_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_TRANSFORMER_FILENAME)
        comp_transformer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_IMPUTER_FILENAME)
        comp_imputer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_NN_MODELS_FILENAME)
        comp_nn_models_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_CLIENTS_IDS_FILENAME)
        comp_clients_ids_filename_tmp = fname + new_suffix + ext

        self.save_object_in_joblib(self.ie_column_imputer, ie_transformer_filename_tmp)
        self.save_object_in_joblib(self.ie_column_tranformer, ie_imputer_filename_tmp)
        self.save_object_in_joblib(self.ie_nnbrs_models, ie_nn_models_filename_tmp)
        self.save_df_in_pickle(self.ie_clients_ids, ie_clients_ids_filename_tmp)
        self.save_object_in_joblib(self.comp_column_imputer, comp_transformer_filename_tmp)
        self.save_object_in_joblib(self.comp_column_tranformer, comp_imputer_filename_tmp)
        self.save_object_in_joblib(self.comp_nnbrs_models, comp_nn_models_filename_tmp)
        self.save_df_in_pickle(self.comp_clients_ids, comp_clients_ids_filename_tmp)

        os.rename(ie_transformer_filename_tmp, IE_TRANSFORMER_FILENAME)
        os.rename(ie_imputer_filename_tmp, IE_IMPUTER_FILENAME)
        os.rename(ie_nn_models_filename_tmp, IE_NN_MODELS_FILENAME)
        os.rename(ie_clients_ids_filename_tmp, IE_CLIENTS_IDS_FILENAME)
        os.rename(comp_transformer_filename_tmp, COMP_TRANSFORMER_FILENAME)
        os.rename(comp_imputer_filename_tmp, COMP_IMPUTER_FILENAME)
        os.rename(comp_nn_models_filename_tmp, COMP_NN_MODELS_FILENAME)
        os.rename(comp_clients_ids_filename_tmp, COMP_CLIENTS_IDS_FILENAME)

    def load_metadata(self):
        self.ie_column_imputer = self.load_joblib_file(IE_IMPUTER_FILENAME)
        self.ie_column_tranformer = self.load_joblib_file(IE_TRANSFORMER_FILENAME)
        self.ie_nnbrs_models = self.load_joblib_file(IE_NN_MODELS_FILENAME)
        self.ie_clients_ids = self.load_pickle_table(IE_CLIENTS_IDS_FILENAME)

        self.comp_column_imputer = self.load_joblib_file(COMP_IMPUTER_FILENAME)
        self.comp_column_tranformer = self.load_joblib_file(COMP_TRANSFORMER_FILENAME)
        self.comp_nnbrs_models = self.load_joblib_file(COMP_NN_MODELS_FILENAME)
        self.comp_clients_ids = self.load_pickle_table(COMP_CLIENTS_IDS_FILENAME)

