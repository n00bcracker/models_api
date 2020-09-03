from common import OracleDB
from config import METAFILES_DIR
import os

import pandas as pd
import joblib
import pickle
import traceback

class MLModel(OracleDB):
    def __init__(self):
        # self.app = app
        #TODO Сделать логирование
        super().__init__()

        if not os.path.isdir(METAFILES_DIR):
            os.mkdir(METAFILES_DIR)

    def predict(self, *args, **kwargs):
        pass

    def fit(self, *args, **kwargs):
        pass

    def load_joblib_file(self, filename):
        try:
            file = joblib.load(filename)
        except FileNotFoundError:
            error = traceback.format_exc()
            print(f'Отсутствует файл "{filename}".\n{error}\n')
            # self.app.logger.error(f'Отсутствует файл "{filename}".\n{errors}\n')
            return None
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise
        else:
            return file

    def save_object_in_joblib(self, object, filename):
        try:
            joblib.dump(object, filename, compress=5, protocol=pickle.DEFAULT_PROTOCOL)
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise

    def load_pickle_table(self, filename):
        try:
            df = pd.read_pickle(filename)
        except FileNotFoundError:
            error = traceback.format_exc()
            print(f'Отсутствует файл "{filename}".\n{error}\n')
            # self.app.logger.error(f'Отсутствует файл "{filename}".\n{errors}\n')
            return None
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise
        else:
            return df

    def save_df_in_pickle(self, df, filename):
        try:
            df.to_pickle(filename, protocol=pickle.DEFAULT_PROTOCOL)
        except Exception:
            error = traceback.format_exc()
            # self.app.logger.error(errors)
            raise

