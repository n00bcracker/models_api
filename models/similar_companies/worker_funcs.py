from models.similar_companies import SimilarCompanies

def fit_task():
    model = SimilarCompanies()
    return model.fit()
