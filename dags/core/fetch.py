# from core.gcs import upload_to_gcs
# from core.bq import load_to_bq
# from core.status import update_status_table
# from datetime import datetime, UTC
# import json

# def fetch_data(api_func, api_args, gcs_bucket = None, bq_table = None, dag_id = None, group_id = None):
#     ''' General fetcher that calls an API function and sends data to gcs and/or bigquery'''
#     return {'sample_data': 'Sample data from core.fetch.fetch_data.  Function design in progress.'}
#     # # Set timestamp variables
#     # gcs_upload_ts = None
#     # bq_upload_ts = None

#     # # Call the API function (should return a dictionary with the data to upload)
#     # data = api_func(**api_args)

#     # # Write to GCS if a bucket is provided
#     # if gcs_bucket:
#     #     upload_to_gcs(data, bucket = gcs_bucket, dag_id = dag_id, group_id = group_id, run_id = None)
#     #     gcs_upload_ts = datetime.now(UTC)
    
#     # # Write to BigQuery if a table is provided
#     # if bq_table:
#     #     load_to_bq(data, table = bq_table, dag_id = dag_id, group_id = group_id)
#     #     bq_upload_ts = datetime.now(UTC)
    
#     # # Update Status Table
#     # if dag_id:
#     #     update_status_table(
#     #         dag_id,
#     #         group_id = group_id,
#     #         api_func = api_func.__name__,
#     #         api_args = json.dumps(api_args),
#     #         gcs_upload_ts = gcs_upload_ts,
#     #         bq_upload_ts = bq_upload_ts
#     #     )
    
#     # return data