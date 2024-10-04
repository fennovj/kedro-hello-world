from kedro.io import DataCatalog
from kedro.config import OmegaConfigLoader

conf_loader = OmegaConfigLoader(conf_source="conf", env='local', base_env='base')
catalog = DataCatalog.from_config(conf_loader.get("catalog"), credentials=conf_loader.get("credentials"))

data = catalog.load("sftable")
print(data.head())