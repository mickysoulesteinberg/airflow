import os, yaml

CONFIG_PATH = os.getenv('CONFIG_PATH', '/opt/airflow/config/settings.yaml')
ENV = os.getenv('ENV', 'dev')

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Environment-specific configurations
CONFIG = config['environments'][ENV]

# Full settings tree if needed for other settings
FULL_CONFIG = config