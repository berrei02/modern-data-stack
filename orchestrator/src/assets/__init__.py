from dagster_dbt import load_assets_from_dbt_project
from dagster import AssetIn, MetadataValue, asset, file_relative_path
import pandas as pd
import httpx
from io import StringIO

@asset(key_prefix=["dbt"], group_name="staging")
def matches_raw():
    def get_atp_match_records_for_given_year_from_remote(year: int) -> pd.DataFrame:
        """
        Reads data from remote csv location.
        The dataset returns for a given year all ATP tennis
        matches and their individual records.
        Data is loaded into a pandas DataFrame and returned
        """
        url = f"https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_{year}.csv"
        response = httpx.get(url)
        if response.status_code != 200:
            raise httpx.RequestError(f"Problems when fetching data from url!: {url}", response.status_code)
        return pd.read_csv(StringIO(response.text))
    
    years = [2018, 2019]

    dfs = []
    for year in years:
        dfs.append(get_atp_match_records_for_given_year_from_remote(year))

    df = pd.concat(dfs)

    df = df.sort_values(by="tourney_id").reset_index(drop=True)
    
    return df



DBT_PROJECT_PATH = file_relative_path(__file__, "../../../tennis_stats")
DBT_PROFILES = file_relative_path(__file__, "../../../tennis_stats")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["dbt"]
)
