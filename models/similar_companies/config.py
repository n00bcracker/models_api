import os
from config import METAFILES_DIR

# Названия таблиц
FULL_MARKET_TABLE = 'si_nn_test'
CLIENTS_TABLE = 'si_nn_train'

SIMCOMP_METADIR = os.path.join(METAFILES_DIR, 'similar_companies')

# Названия файлов с обученными преобразованиями и моделями
IMPUTER_FILENAME = os.path.join(SIMCOMP_METADIR, 'imputing.joblib.gz')
TRANSFORMER_FILENAME = os.path.join(SIMCOMP_METADIR, 'transforming.joblib.gz')
NN_MODELS_FILENAME = os.path.join(SIMCOMP_METADIR, 'knn_models.joblib.gz')
CLIENTS_IDS_FILENAME = os.path.join(SIMCOMP_METADIR, 'client_okved_idxs.pkl.gz')

# Названия использующихся стобцов таблицы по категориям
ID_COLS = ['client_key', 'okved_code', ]
CONT_FEATURES_COLS = ['city_population', 'charter_capital', 'revenue_all', 'workers_range', 'post_office_latitude', \
                      'post_office_longitude', 'si_coowner_egrul_cnt', ]
ORDER_FEATURES_COLS = ['age', 'okved_cnt', 'cnt_comp_in_group', ]
BIN_CATEG_COLS = []
CATEG_FEATURES_COLS = ['okfs_code', 'city', 'okved_code4']