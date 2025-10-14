from core.gcs import list_gcs_files
import re

def parse_gcs_input(input_str):
    if input_str.endswith('/'):
        # Folder prefix, list all files under it
        return list_gcs_files(input_str)
    if  '*' in input_str:
        # Wildcard path, list matching files
        prefix = input_str.split('*')[0]
        all_files = list_gcs_files(prefix)
        pattern = re.compile(input_str.replace('*', '.*'))
        return [f for f in all_files if pattern.match(f)]
    else:
        # Single file path
        return [input_str]