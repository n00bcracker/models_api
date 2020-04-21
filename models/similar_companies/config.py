import os
# Названия таблиц
FULL_MARKET_TABLE = 'si_nn_test'
CLIENTS_TABLE = 'si_nn_train'

# Названия файлов с обученными преобразованиями и моделями
IMPUTER_FILENAME = '/home/ml_model/models/similar_companies/meta/imputing.joblib.gz'
TRANSFORMER_FILENAME = '/home/ml_model/models/similar_companies/meta/transforming.joblib.gz'
NN_MODELS_FILENAME = '/home/ml_model/models/similar_companies/meta/knn_models.joblib.gz'
CLIENTS_IDS_FILENAME = '/home/ml_model/models/similar_companies/meta/client_okved_idxs.pkl.gz'

# Названия использующихся стобцов таблицы по категориям
ID_COLS = ['client_key', 'okved_code', ]
CONT_FEATURES_COLS = ['city_population', 'charter_capital', 'revenue_all', 'workers_range', 'post_office_latitude', \
                      'post_office_longitude', 'si_coowner_egrul_cnt', ]
ORDER_FEATURES_COLS = ['age', 'okved_cnt', 'cnt_comp_in_group', ]
BIN_CATEG_COLS = []
CATEG_FEATURES_COLS = ['okfs_code', 'city', ]