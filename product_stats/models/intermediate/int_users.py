"""
This just illustrates the use of a python model
inside the dbt DAG.
You can use any data science library (scikit-learn, ..)
in here to achieve e.g. k-means, regressions, ...

To run python in collab with postgres, you need to have
dbt-fal adapter configured and installed dependencies.
"""
import pandas as pd
from random import randint

def model(dbt, session):
    df = dbt.ref("stg_users")
    df["predicted_user_category"] = randint(1, 10)  # Mocked: random prediction
    return df
