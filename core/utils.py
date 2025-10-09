from datetime import datetime, UTC

def bq_current_timestamp():
    return datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')