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

def main(event, context):
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
    df.apply(lambda x: x.apply(lambda x: x.lower()) if hasattr(x[0], 'lower') else x)
    df[["start_ts","end_ts","cell_last_known_ts"]] = df[["start_ts","end_ts","cell_last_known_ts"]].applymap(lambda x: datetime.datetime.fromtimestamp(x))
    job = bq.load_table_from_dataframe(df, f"coherence-proto.ambri.anomaly", job_config=job_config)
    job.result()
      