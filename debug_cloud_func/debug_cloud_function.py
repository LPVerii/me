from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import datetime
import fs_gcsfs as GCS  # TODO

bq = bigquery.Client()
cs = storage.Client()

schema_for_anomaly = [
bigquery.SchemaField("cell_id", "STRING"),
bigquery.SchemaField("start_ts", "DATETIME"),
bigquery.SchemaField("end_ts", "DATETIME"),
bigquery.SchemaField("class", "INTEGER"),
bigquery.SchemaField("severity", "FLOAT"),
bigquery.SchemaField("anomaly_id", "STRING"),
bigquery.SchemaField("cell_last_known_ts", "DATETIME"),
bigquery.SchemaField("run_id", "INTEGER"),
]

job_config = bigquery.LoadJobConfig(
  schema=schema_for_anomaly,
  source_format=bigquery.SourceFormat.CSV
  )
# job_config.skip_leading_rows = 1

def main(event, context = None):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    blob_name = event['name']
    if not blob_name.startswith('model_output'):
      return 0
    source_bucket = event['bucket']
    gs_path = f'gs://{source_bucket}/{blob_name}'
    
    print(f"Event in funcion: {event}")
    print(f"Blob name: {blob_name}")
    print(f"Path: {gs_path}")

    df = pd.read_csv(gs_path)
    # df = df.drop(df.columns[0],axis=1)
    df = df.apply(lambda x: x.apply(lambda x: x.lower()) if hasattr(x[0], 'lower') else x)
    df[["start_ts","end_ts","cell_last_known_ts"]] = df[["start_ts","end_ts","cell_last_known_ts"]].applymap(lambda x: datetime.datetime.fromtimestamp(x))
    job = bq.load_table_from_dataframe(df, f"coherence-proto.ambri.anomaly", job_config=job_config)
    job.result()
      

main({'bucket': 'test_for_function', 'contentType': 'text/csv', 'crc32c': 'xwG1IQ==', 'etag': 'CIHW3NS9mP4CEAE=', 'generation': '1680894953204481', 'id': 'test_for_function/model_output/echem_c4-31110.csv/1680894953204481', 'kind': 'storage#object', 'md5Hash': 'h06uPToaaVo8FFNibkgzyw==', 'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/test_for_function/o/model_output%2Fechem_c4-31110.csv?generation=1680894953204481&alt=media', 'metageneration': '1', 'name': 'model_output/echem_c4-31110.csv', 'selfLink': 'https://www.googleapis.com/storage/v1/b/test_for_function/o/model_output%2Fechem_c4-31110.csv', 'size': '1728', 'storageClass': 'STANDARD', 'timeCreated': '2023-04-07T19:15:53.281Z', 'timeStorageClassUpdated': '2023-04-07T19:15:53.281Z', 'updated': '2023-04-07T19:15:53.281Z'})