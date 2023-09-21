from dataclasses import dataclass

import yaml


@dataclass(kw_only=True)
class Config:
    api_key: str
    api_secret: str
    api_passphrase: str

    @classmethod
    def read_config(cls, path):
        with open(path, 'r') as f:
            raw_conf = yaml.safe_load(f)
            return cls(**raw_conf)
