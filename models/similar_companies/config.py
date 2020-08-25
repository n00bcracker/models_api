import os
from config import METAFILES_DIR

# Названия таблиц
FULL_MARKET_IE_TABLE = 'si_nn_ie_test'
FULL_MARKET_COMP_TABLE = 'si_nn_test'

CLIENTS_IE_TABLE = 'si_nn_ie_train'
CLIENTS_COMP_TABLE = 'si_nn_train'

SIMCOMP_METADIR = os.path.join(METAFILES_DIR, 'similar_companies')

# Названия файлов с обученными преобразованиями и моделями
IE_IMPUTER_FILENAME = os.path.join(SIMCOMP_METADIR, 'ie_imputing.joblib.gz')
IE_TRANSFORMER_FILENAME = os.path.join(SIMCOMP_METADIR, 'ie_transforming.joblib.gz')
IE_NN_MODELS_FILENAME = os.path.join(SIMCOMP_METADIR, 'ie_knn_models.joblib.gz')
IE_CLIENTS_IDS_FILENAME = os.path.join(SIMCOMP_METADIR, 'ie_client_okved_idxs.pkl.gz')

COMP_IMPUTER_FILENAME = os.path.join(SIMCOMP_METADIR, 'comp_imputing.joblib.gz')
COMP_TRANSFORMER_FILENAME = os.path.join(SIMCOMP_METADIR, 'comp_transforming.joblib.gz')
COMP_NN_MODELS_FILENAME = os.path.join(SIMCOMP_METADIR, 'comp_knn_models.joblib.gz')
COMP_CLIENTS_IDS_FILENAME = os.path.join(SIMCOMP_METADIR, 'comp_client_okved_idxs.pkl.gz')

# Названия использующихся стобцов таблицы по категориям
COMP_ID_COLS = ['client_key', 'inn', 'kpp', 'okved_code',]
COMP_CONT_FEATURES_COLS = ['city_population', 'charter_capital', 'revenue_all', 'workers_range', 'post_office_latitude',
                            'post_office_longitude', 'si_coowner_egrul_cnt', ]
COMP_ORDER_FEATURES_COLS = ['age', 'okved_cnt', 'cnt_comp_in_group', ]
COMP_BIN_CATEG_COLS = []
COMP_CATEG_FEATURES_COLS = ['okfs_code', 'city', 'okved_code4', ]

IE_ID_COLS = ['client_key', 'inn', 'kpp', 'okved_code',]
IE_CONT_FEATURES_COLS = ['city_population', 'revenue_all', 'post_office_latitude', 'post_office_longitude', ]
IE_ORDER_FEATURES_COLS = ['age', 'okved_cnt', 'cnt_comp_in_group', ]
IE_BIN_CATEG_COLS = ['sex_code', ]
IE_CATEG_FEATURES_COLS = ['city', 'okved_code4', ]