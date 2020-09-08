import os
from config import METAFILES_DIR

# Названия таблиц
FULL_MARKET_IE_TABLE = os.getenv('FULL_MARKET_IE_TABLE') or None
FULL_MARKET_COMP_TABLE = os.getenv('FULL_MARKET_COMP_TABLE') or None

BLOCKED_IE_TABLE = os.getenv('BLOCKED_IE_TABLE') or None
BLOCKED_COMP_TABLE = os.getenv('BLOCKED_COMP_TABLE') or None

ENTRY_COMPLIANCE_IE_TABLE = os.getenv('ENTRY_COMPLIANCE_IE_TABLE') or None
ENTRY_COMPLIANCE_COMP_TABLE = os.getenv('ENTRY_COMPLIANCE_COMP_TABLE') or None

ENTCOMP_METADIR = os.path.join(METAFILES_DIR, 'entry_compliance')

# Названия файлов с обученными преобразованиями и моделями
IE_IMPUTER_TRANSFORMER_FILENAME = os.path.join(ENTCOMP_METADIR, 'ie_imp_transf.joblib.gz')
IE_COMPL_MODEL_FILENAME = os.path.join(ENTCOMP_METADIR, 'ie_compl_model.joblib.gz')

COMP_IMPUTER_TRANSFORMER_FILENAME = os.path.join(ENTCOMP_METADIR, 'comp_imp_transf.joblib.gz')
COMP_COMPL_MODEL_FILENAME = os.path.join(ENTCOMP_METADIR, 'comp_compl_model.joblib.gz')

# Названия использующихся стобцов таблицы по категориям
IE_CATEG_FEATURES_COLS = ['reg_region', 'okopf_code', 'main_okved', 'okved2', 'country', 'message_id_type']
COMP_CATEG_FEATURES_COLS = ['okfs_code', 'okogu_code', 'rts', 'fcsm_code', 'workers_range', 'company_size_description',
                            'consolidated_ind_value', 'payment_index_desc', 'financebal_type', 'company_type_code',
                            'okved2', 'main_okved', 'post_code', 'reg', 'city']

