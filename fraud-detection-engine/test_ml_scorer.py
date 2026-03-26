from app.core.ml_scorer import MLScorer

payload = {
    "gender": 1,
    "age": 45,
    "income": 7500,
    "household_size": 2,
    "nb_children": 1,
    "vehicles_owned": 1,
    "dependency_ratio": 1.0,
    "income_per_person": 3750,
    "disability_flag": 0,
    "immigration_flag": 0,
    "own_home_flag": 1,
    "shared_phone_count": 1,
    "shared_account_count": 1,
}

scorer = MLScorer(model_name="random_forest")
result = scorer.score(payload)

print(result)