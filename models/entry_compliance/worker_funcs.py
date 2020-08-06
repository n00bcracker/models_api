from models.entry_compliance import EntryCompliance

def fit_task():
    model = EntryCompliance()
    return model.fit()

def predict_all_task():
    model = EntryCompliance()
    return model.predict()
