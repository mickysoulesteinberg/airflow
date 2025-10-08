def make_gcs_format_strings(context):
    '''Returns format strings which includes Dag and date context, but leaves placeholders for dynamic parts'''
    dag_id = context['dag'].dag_id
    ds_nodash = context['ds_nodash']
    prefix = f'{dag_id}/{{api}}/{{api_path}}'
    file_name = f'{{call_id}}-{ds_nodash}.json'
    return {'prefix': prefix, 'file_name': file_name}

def make_gcs_path_factory(context):
    '''Returns callables that use current Airflow context, but allow dynamic inputs later'''
    format_strings = make_gcs_format_strings(context)

    def _make_prefix(api, api_path):
        return format_strings['prefix'].format(api = api, api_path = api_path)
    
    def _make_file_name(call_id = 'data'):
        return format_strings['file_name'].format(call_id = call_id)
    
    return _make_prefix, _make_file_name
