from config.datasources.config_factory import create_config, BQ_METADATA_COL, BQ_TIMESTAMP_COL
import yaml
from pathlib import Path

OUTPUT_FILE_PATH = Path('_scratch_dbt/configs.yaml')

# Parameters
RAW_DATA_KEY = 'data'


# TMDB
from config.datasources import tmdb
TMDB_DISCOVER_MOVIES = create_config('tmdb', tmdb.DISCOVER_MOVIES_RAW)
TMDB_CREDITS = create_config('tmdb', tmdb.CREDITS_RAW)

TMDB_CONFIG = {'discover_movies': TMDB_DISCOVER_MOVIES, 'credits': TMDB_CREDITS}


# SSA
from config.datasources import ssa
SSA_NAMES = create_config('ssa', ssa.SSA_NAMES_RAW)
SSA_STATE_NAMES = create_config('ssa', ssa.SSA_STATE_NAMES_RAW)
SSA_TERRITORY_NAMES = create_config('ssa', ssa.SSA_TERRITORY_NAMES_RAW)

SSA_CONFIG = {'names': SSA_NAMES, 'state_names': SSA_STATE_NAMES, 'territory_names': SSA_TERRITORY_NAMES}

# # SPOTIFY
# from config.datasources import spotify
# SPOTIFY_ARTISTS = create_config('spotify', spotify.ARTISTS_RAW)

def main():
    config = {'tmdb': TMDB_CONFIG}
    config['ssa'] = SSA_CONFIG
    OUTPUT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE_PATH, 'w') as f:
        yaml.dump(config, f)
    
if __name__ == '__main__':
    main()