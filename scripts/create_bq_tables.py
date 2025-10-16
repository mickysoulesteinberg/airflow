from pipeline.create_table import create_table_from_config
from schemas import ALL_TABLES

if __name__ == '__main__':
    # set to True to rebuild all tables from scratch
    FORCE_RECREATE = False

    for dataset_table, table_config in ALL_TABLES.items():
        create_table_from_config(dataset_table, table_config, force_recreate = FORCE_RECREATE)
        
