# Core Airflow classes
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Additional libraries for email handling
import imaplib
import email
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
}

with DAG(
    'test_email_to_gcs',
    default_args=default_args,
    description='A simple test DAG to read email and upload to GCS',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={
        'dataset': 'airflow_practice',
        'table': 'email_data'
    }
) as dag:

    def extract_email_csv(**context):
        """
        Connect to Gmail with IMAP, search for unread emails,
        download the first CSV attachment, and save it locally.
        """

        # Required environment variables for Gmail credentials (fail if not set)
        gmail_user = os.environ['GMAIL_USER']
        gmail_password = os.environ['GMAIL_PASSWORD']

        # Optional: allow overriding save directory, save to /tmp by default
        save_dir = os.environ.get('SAVE_DIR', '/tmp')

        # Connect to Gamil
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(gmail_user, gmail_password)

        # Look at inbox
        mail.select('inbox')

        # Search for unread emails
        status, data = mail.search(None, 'ALL') # 'UNSEEN' for unread only
        mail_ids = data[0].split() # split into individual IDs
        if not mail_ids:
            raise ValueError('No unread emails found')

        # Fetch the latest email
        latest_email_id = mail_ids[-1]
        status, msg_data = mail.fetch(latest_email_id, '(RFC822)')  # full message
        msg = email.message_from_bytes(msg_data[0][1])

        # Loop through email parts to find CSV attachment
        for part in msg.walk():
            if part.get_content_disposition() == 'attachment':
                file_name = part.get_filename()
                print(file_name)
                if file_name and file_name.endswith('.csv'):
                    file_path = os.path.join(save_dir, file_name)
                    with open(file_path, 'wb') as f:
                        f.write(part.get_payload(decode=True))
                    print(f"Saved attachment to {file_path}")

                    # Push file path to XCom for downstream tasks
                    context['ti'].xcom_push(key='csv_path', value=file_path)
                    return file_path
        
        raise ValueError('No CSV attachment found in the latest unread email')


    read_task = PythonOperator(
        task_id='extract_email_csv',
        python_callable=extract_email_csv,
    )

    def load_to_bq(**context):
        """
        Load the CSV file saved by the previous task into BigQuery.
        """

        from google.cloud import bigquery

        # Get CVS file path from XCom
        ti = context['ti']

        csv_path = ti.xcom_pull(task_ids='extract_email_csv', key='csv_path')
        if not csv_path:
            raise ValueError('No CSV file path found in XCom')
        
        # Required environment variables for GCP 
        gcp_project_id = os.environ['GCP_PROJECT_ID']
        gcp_dataset = context['params']['dataset']
        gcp_table = context['params']['table']
        credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

        # Initialize BigQuery client
        client = bigquery.Client.from_service_account_json(credentials_path, project=gcp_project_id)

        # Define target table
        table_id = f'{gcp_project_id}.{gcp_dataset}.{gcp_table}'

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition='WRITE_APPEND'  # Append to existing table
        )

        # Load data from CSV file to BigQuery
        with open(csv_path, 'rb') as source_file:
            load_job = client.load_table_from_file(
                source_file,
                table_id,
                job_config = job_config
            )

        load_job.result()  # Wait for the job to complete

        print(f'Loaded {load_job.output_rows} rows into {table_id} from {csv_path}')



    
    load_task = PythonOperator(
        task_id='load_to_bq',
        python_callable=load_to_bq
    )

    def cleanup_temp_file(**context):
        # Retrieve file path from XCom
        ti = context['ti']
        file_path = ti.xcom_pull(key='csv_path', task_ids='extract_email_csv')
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted temporary file: {file_path}")
        else:
            print("No temporary file to delete.")
    
    cleanup_task = PythonOperator(
        task_id='cleanup_temp_file',
        python_callable=cleanup_temp_file,
        provide_context=True,
        trigger_rule='all_done'  # Ensure this runs even if previous tasks fail
    )

    read_task >> load_task >> cleanup_task