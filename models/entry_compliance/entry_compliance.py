from common import MLModel
from models.entry_compliance.config import ENTRY_COMPLIANCE_COMP_TABLE, ENTRY_COMPLIANCE_IE_TABLE
from models.entry_compliance.config import FULL_MARKET_IE_TABLE, FULL_MARKET_COMP_TABLE
from models.entry_compliance.config import BLOCKED_IE_TABLE, BLOCKED_COMP_TABLE
from models.entry_compliance.config import IE_CATEG_FEATURES_COLS, COMP_CATEG_FEATURES_COLS
from models.entry_compliance.config import IE_IMPUTER_TRANSFORMER_FILENAME, COMP_IMPUTER_TRANSFORMER_FILENAME
from models.entry_compliance.config import IE_COMPL_MODEL_FILENAME, COMP_COMPL_MODEL_FILENAME
from models.entry_compliance.config import ENTCOMP_METADIR
from utils import resources, check_inn

import os
import pandas as pd
import numpy as np
import datetime
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.compose import make_column_transformer, ColumnTransformer
from xgboost.sklearn import XGBClassifier

import traceback

class EntryCompliance(MLModel):
    def __init__(self):
        super().__init__()
        self.full_market_ie_tablename = FULL_MARKET_IE_TABLE
        self.full_market_comp_tablename = FULL_MARKET_COMP_TABLE
        self.blocked_ie_tablename = BLOCKED_IE_TABLE
        self.blocked_comp_tablename = BLOCKED_COMP_TABLE

        self.pred_res_ie_tablename = ENTRY_COMPLIANCE_IE_TABLE
        self.pred_res_comp_tablename = ENTRY_COMPLIANCE_COMP_TABLE

        if not os.path.isdir(ENTCOMP_METADIR):
            os.mkdir(ENTCOMP_METADIR)

        self.load_metadata()

    def preprocess_ie_table(self, df):
        # убираем ненужные столбцы
        df = df.drop(columns=['ddate', 'reg_data', 'okpo_code', 'okato_code', 'oktmo_code',])

        # Преобразуем пропуски к одному виду
        df = df.fillna(np.nan)
        df.loc[:, IE_CATEG_FEATURES_COLS] = df.loc[:, IE_CATEG_FEATURES_COLS].convert_dtypes(convert_string=False)
        return df

    def preprocess_comp_table(self, df):
        df = df.drop(columns=['ddate', 'okpo_code', 'oktmo_code',]) # убираем ненужные столбцы

        # Преобразуем пропуски к одному виду
        df = df.fillna(np.nan)
        df.loc[:, COMP_CATEG_FEATURES_COLS] = df.loc[:, COMP_CATEG_FEATURES_COLS].convert_dtypes(convert_string=False)
        return df
    
    def get_block_predict(self, inn):
        if len(inn) == 10:
            tablename = self.pred_res_comp_tablename
        else:
            tablename = self.pred_res_ie_tablename

        sql_query = f"""
                        select
                            t.ddate,
                            t.inn,
                            t.kpp,
                            t.prog_result,
                            t.prog_prob
                                from {tablename} t
                                    where 1=1
                                    and t.ddate = (select max(ddate) from {tablename})
                                    and t.inn = :inn
                    """

        compl_df = self.read_sql_query(sql_query, params=[inn, ])
        return compl_df

    def get_predict(self, inn, kpp=None):
        res = dict()
        res['inn'] = inn
        res['kpp'] = kpp
        if check_inn(inn):
            compl_df = self.get_block_predict(inn)
            if compl_df is None:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка исполнения SQL-запроса'
            elif compl_df.shape[0] == 0:
                res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданного ИНН'
            elif compl_df.shape[0] > 1:
                if kpp is None:
                    res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                    res[resources.RESPONSE_ERROR_FIELD] = 'Для переданного ИНН имеется несколько КПП'
                else:
                    compl_df = compl_df.loc[compl_df.kpp == kpp, :]
                    if compl_df.shape[0] == 0:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Error'
                        res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для переданных ИНН и КПП'
                    else:
                        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                        res['will_be_blocked_in_3m'] = True if compl_df.prog_result[0] == 1 else False
                        res['block_probability'] = compl_df.prog_prob[0]

            else:
                res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
                res['will_be_blocked_in_3m'] = True if compl_df.prog_result[0] == 1 else False
                res['block_probability'] = compl_df.prog_prob[0]
        else:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Некорректный ИНН'

        return res

    def fit(self):
        res = dict()

        # Считываем таблицу со всеми ИП, чтобы обучить преобразователь
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

        full_market_ie = full_market_ie.drop(columns=['current_flag',])
        all_cols = set(full_market_ie.columns)
        rest_cols = list(all_cols.difference(IE_CATEG_FEATURES_COLS))

        ie_cols_names = IE_CATEG_FEATURES_COLS + rest_cols
        full_market_ie = full_market_ie.loc[:, ie_cols_names]

        ie_categ_imputer_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='')),
            ('encoder', OrdinalEncoder()),
        ])

        self.ie_column_imputer_transformer = make_column_transformer(
            (ie_categ_imputer_transformer, IE_CATEG_FEATURES_COLS),
            remainder='passthrough',
            n_jobs=None
        )

        self.ie_column_imputer_transformer.fit(full_market_ie)
        del full_market_ie

        # Считываем таблицу с блокировками ИП для обучения
        training_ie = self.read_sql_table(self.blocked_ie_tablename)
        if training_ie is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif training_ie.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            training_ie = self.preprocess_ie_table(training_ie)

        training_ie = training_ie.loc[:, ie_cols_names]
        training_ie = pd.DataFrame(self.ie_column_imputer_transformer.transform(training_ie), columns=ie_cols_names)

        y_train_ie = training_ie.loc[:, 'block_flag']
        x_train_ie = training_ie.drop(columns=['inn', 'block_flag']).astype(float)

        self.ie_compl_model = XGBClassifier(base_score=0.01, booster='gbtree', colsample_bylevel=1,
                                    colsample_bytree=0.75, gamma=0, learning_rate=0.1, max_delta_step=0,
                                    max_depth=7, min_child_weight=5, missing=None, n_estimators=200,
                                    n_jobs=1, nthread=None, objective='binary:logistic', random_state=0,
                                    reg_alpha=0, reg_lambda=1, scale_pos_weight=1, seed=None,
                                    silent=True, subsample=0.6)
        self.ie_compl_model.fit(x_train_ie, y_train_ie)
        del training_ie

        # Считываем таблицу со всеми ООО, чтобы обучить преобразователь
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

        full_market_comp = full_market_comp.drop(columns=['current_flag',])
        all_cols = set(full_market_comp.columns)
        rest_cols = list(all_cols.difference(COMP_CATEG_FEATURES_COLS))

        comp_cols_names = COMP_CATEG_FEATURES_COLS + rest_cols
        full_market_comp = full_market_comp.loc[:, comp_cols_names]

        comp_categ_imputer_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='')),
            ('encoder', OrdinalEncoder()),
        ])

        self.comp_column_imputer_transformer = make_column_transformer(
            (comp_categ_imputer_transformer, COMP_CATEG_FEATURES_COLS),
            remainder='passthrough',
            n_jobs=None
        )

        self.comp_column_imputer_transformer.fit(full_market_comp)
        del full_market_comp

        # Считываем таблицу с блокировками ООО для обучения
        training_comp = self.read_sql_table(self.blocked_comp_tablename)
        if training_comp is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif training_comp.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для обучения'
            return res
        else:
            training_comp = self.preprocess_comp_table(training_comp)

        training_comp = training_comp.loc[:, comp_cols_names]
        training_comp = pd.DataFrame(self.comp_column_imputer_transformer.transform(training_comp),
                                                                                                columns=comp_cols_names)

        y_train_comp = training_comp.loc[:, 'block_flag']
        x_train_comp = training_comp.drop(columns=['client_key', 'block_flag']).astype(float)

        self.comp_compl_model = XGBClassifier(base_score=0.01, booster='gbtree', colsample_bylevel=1,
                                    colsample_bytree=0.75, gamma=0, learning_rate=0.1, max_delta_step=0,
                                    max_depth=7, min_child_weight=5, missing=None, n_estimators=200,
                                    n_jobs=1, nthread=None, objective='binary:logistic', random_state=0,
                                    reg_alpha=0, reg_lambda=1, scale_pos_weight=1, seed=None,
                                    silent=True, subsample=0.6)
        self.comp_compl_model.fit(x_train_comp, y_train_comp)
        del training_comp

        # Сохраняем обученные данные
        self.save_metadata()

        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'
        return  res

    def predict(self):
        res = dict()

        sql_query = """
                        select *
                                from {tablename} t
                                    where 1=1
                                    and t.current_flag = 1
                    """


        # Предсказание блокировок для ИП
        if self.ie_column_imputer_transformer is None or self.ie_compl_model is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Модель не обучена, предсказания невозможны'
            return res

        full_active_ie = self.read_sql_query(sql_query.format(tablename = self.full_market_ie_tablename))
        if full_active_ie is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif full_active_ie.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для предсказания'
            return res
        else:
            full_active_ie = self.preprocess_ie_table(full_active_ie)

        full_active_ie = full_active_ie.drop(columns=['current_flag',])
        all_cols = set(full_active_ie.columns)
        rest_cols = list(all_cols.difference(IE_CATEG_FEATURES_COLS))

        ie_cols_names = IE_CATEG_FEATURES_COLS + rest_cols
        full_active_ie = full_active_ie.loc[:, ie_cols_names]

        full_active_ie = pd.DataFrame(self.ie_column_imputer_transformer.transform(full_active_ie),
                                                                                                columns=ie_cols_names)
        ie_inns = full_active_ie.loc[:, 'inn']
        x_pred_ie = full_active_ie.drop(columns=['inn', 'block_flag']).astype(float)

        ie_pred_block = self.ie_compl_model.predict(x_pred_ie)
        ie_pred_prob = self.ie_compl_model.predict_proba(x_pred_ie)

        del full_active_ie
        ie_pred_result = pd.DataFrame({'inn': ie_inns, 'prog_result': ie_pred_block, 'prog_prob': ie_pred_prob[:, 1]})

        value_day = datetime.date.today()
        ie_pred_result['value_day'] = value_day

        self.save_df_in_sql_table(ie_pred_result, self.pred_res_ie_tablename)

        # Предсказание блокировок для ООО
        if self.comp_column_imputer_transformer is None or self.comp_compl_model is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Модель не обучена, предсказания невозможны'
            return res

        full_active_comp = self.read_sql_query(sql_query.format(tablename = self.full_market_comp_tablename))
        if full_active_comp is None:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Ошибка выполнения SQL-запроса'
            return res
        elif full_active_comp.shape[0] == 0:
            res[resources.RESPONSE_STATUS_FIELD] = 'Error'
            res[resources.RESPONSE_ERROR_FIELD] = 'Отсутствуют данные для предсказания'
            return res
        else:
            full_active_comp = self.preprocess_comp_table(full_active_comp)

        full_active_comp = full_active_comp.drop(columns=['current_flag',])
        all_cols = set(full_active_comp.columns)
        rest_cols = list(all_cols.difference(COMP_CATEG_FEATURES_COLS))

        comp_cols_names = COMP_CATEG_FEATURES_COLS + rest_cols
        full_active_comp = full_active_comp.loc[:, comp_cols_names]

        full_active_comp = pd.DataFrame(self.comp_column_imputer_transformer.transform(full_active_comp),
                                            columns=ie_cols_names)
        comp_clkey = full_active_comp.loc[:, ['client_key', ]]
        x_pred_comp = full_active_comp.drop(columns=['client_key', 'block_flag']).astype(float)

        comp_clkey['inn'] = comp_clkey.client_key.str[0:10]
        comp_clkey['kpp'] = comp_clkey.client_key.str[11:]

        comp_pred_block = self.comp_compl_model.predict(x_pred_comp)
        comp_pred_prob = self.comp_compl_model.predict_proba(x_pred_comp)

        del full_active_comp
        comp_pred_result = pd.DataFrame({'inn': comp_clkey.inn, 'kpp': comp_clkey.kpp,
                                                    'prog_result': comp_pred_block, 'prog_prob': comp_pred_prob[:, 1]})
        comp_pred_result['value_day'] = value_day

        self.save_df_in_sql_table(comp_pred_result, self.pred_res_comp_tablename)


        res[resources.RESPONSE_STATUS_FIELD] = 'Ok'

    def save_metadata(self):
        new_suffix = '.new'

        fname, ext = os.path.splitext(IE_IMPUTER_TRANSFORMER_FILENAME)
        ie_imputer_transformer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(IE_COMPL_MODEL_FILENAME)
        ie_compl_model_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_IMPUTER_TRANSFORMER_FILENAME)
        comp_imputer_transformer_filename_tmp = fname + new_suffix + ext
        fname, ext = os.path.splitext(COMP_COMPL_MODEL_FILENAME)
        comp_compl_model_filename_tmp = fname + new_suffix + ext

        self.save_object_in_joblib(self.ie_column_imputer_transformer, ie_imputer_transformer_filename_tmp)
        self.save_object_in_joblib(self.ie_compl_model, ie_compl_model_filename_tmp)
        self.save_object_in_joblib(self.comp_column_imputer_transformer, comp_imputer_transformer_filename_tmp)
        self.save_object_in_joblib(self.comp_compl_model, comp_compl_model_filename_tmp)

        os.rename(ie_imputer_transformer_filename_tmp, IE_IMPUTER_TRANSFORMER_FILENAME)
        os.rename(ie_compl_model_filename_tmp, IE_COMPL_MODEL_FILENAME)
        os.rename(comp_imputer_transformer_filename_tmp, COMP_IMPUTER_TRANSFORMER_FILENAME)
        os.rename(comp_compl_model_filename_tmp, COMP_COMPL_MODEL_FILENAME)

    def load_metadata(self):
        self.ie_column_imputer_transformer = self.load_joblib_file(IE_IMPUTER_TRANSFORMER_FILENAME)
        self.ie_compl_model = self.load_joblib_file(IE_COMPL_MODEL_FILENAME)
        self.comp_column_imputer_transformer = self.load_joblib_file(COMP_IMPUTER_TRANSFORMER_FILENAME)
        self.comp_compl_model = self.load_joblib_file(COMP_COMPL_MODEL_FILENAME)
