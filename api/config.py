from dynaconf import Dynaconf

settings_api = Dynaconf(
    settings_files=['settings.toml', '.secrets.toml'],
) 

