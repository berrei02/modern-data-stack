from dagster_dbt import load_assets_from_dbt_project
from dagster import AssetIn, MetadataValue, asset, file_relative_path
from ..utils.postgres_handler import PostgresConnector
import pandas as pd
import httpx
from io import StringIO

@asset(key_prefix=["dbt"], group_name="raw")
def matches():
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
    PostgresConnector().handle_output(df=df, table="matches", schema="raw")
    return df



DBT_PROJECT_PATH = file_relative_path(__file__, "../../../tennis_stats")
DBT_PROFILES = file_relative_path(__file__, "../../../tennis_stats")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["dbt"]
)
