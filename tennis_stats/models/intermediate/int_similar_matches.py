import pandas as pd

N_CLUSTERS = 4

def model(dbt, session):
    """
    Get's data from stg_matches and clusters i.t.o. similarity
    to find clusters of likely similar matches.
    Storing a dataframe with match_ids and clusters.
    """
    df = dbt.ref("stg_matches")
    filtered_df = df.query("~(age_delta.isna() or rank_delta.isna())").reset_index()
    filtered_df["age_delta_std"] = (
        filtered_df["age_delta"] - filtered_df["age_delta"].mean()
    ) / filtered_df["age_delta"].std()
    filtered_df["rank_delta_std"] = (
        filtered_df["rank_delta"] - filtered_df["rank_delta"].mean()
    ) / filtered_df["rank_delta"].std()

    filtered_df = filtered_df[
        ["match_id", "age_delta_std", "rank_delta_std"]
    ].reset_index(drop=True)

    from sklearn.cluster import KMeans

    km = KMeans(
        n_clusters=N_CLUSTERS, init="random", n_init=10, max_iter=300, tol=1e-04, random_state=0
    )
    y_km = km.fit_predict(filtered_df.drop(columns="match_id"))

    filtered_df["cluster"] = pd.Series(y_km)

    return filtered_df
