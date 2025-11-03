from config.api_factory import create_config, BQ_METADATA_COL, BQ_TIMESTAMP_COL


# Parameters
RAW_DATA_KEY = 'data'

# TMDB
from config.datasources_og import tmdb
TMDB_DISCOVER_MOVIES = create_config('tmdb', tmdb.DISCOVER_MOVIES_RAW)
TMDB_CREDITS = create_config('tmdb', tmdb.CREDITS_RAW)

# SSA
from config.datasources_og import ssa
SSA_NAMES = create_config('ssa', ssa.SSA_NAMES_RAW)
SSA_STATE_NAMES = create_config('ssa', ssa.SSA_STATE_NAMES_RAW)
SSA_TERRITORY_NAMES = create_config('ssa', ssa.SSA_TERRITORY_NAMES_RAW)

# SPOTIFY
from config.datasources_og import spotify
SPOTIFY_ARTISTS = create_config('spotify', spotify.ARTISTS_RAW)