# /tutorial_template/tutorial_dbt_dagster/__init__.py

import os

from dagster_dbt import DbtCliClientResource
from src import assets
from src.assets import DBT_PROFILES, DBT_PROJECT_PATH

from dagster import Definitions, load_assets_from_modules

resources = {
    "dbt": DbtCliClientResource(
        project_dir=DBT_PROJECT_PATH,
        profiles_dir=DBT_PROFILES,
    ),
}

defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)
