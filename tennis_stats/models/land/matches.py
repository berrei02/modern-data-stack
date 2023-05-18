import pandas as pd
import httpx
from io import StringIO

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

def model(dbt, session):
    years = [2020, 2021, 2022]

    dfs = []
    for year in years:
        dfs.append(get_atp_match_records_for_given_year_from_remote(year))

    df = pd.concat(dfs)

    df = df.sort_values(by="tourney_id").reset_index(drop=True)
    
    return df