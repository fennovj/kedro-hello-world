from kedro.io import DataCatalog
from kedro.config import OmegaConfigLoader

# Reading data from configuration file

conf_loader = OmegaConfigLoader(conf_source="conf", env='local', base_env='base')
catalog = DataCatalog.from_config(conf_loader.get("catalog"), credentials=conf_loader.get("credentials"))

data = catalog.load("sftable")
print(data.head())

# Reading data directly

from src.fennotest import SnowflakeQueryDataset

sf_credentials = conf_loader.get("credentials")['snowflake']

snowflake_dataset = SnowflakeQueryDataset(
    credentials=sf_credentials,
    account="ACCOUNT.west-europe.azure",
    query="SELECT * FROM DATABASE.SCHEMA.TBLNAME LIMIT 10"
)

data = snowflake_dataset.load()

print(data.head())