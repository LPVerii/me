from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import datetime
from lantern_utils.connect_bigquery import ConnectBigQuery

bq = ConnectBigQuery()
# cs = storage.Client()

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

schema_for_feedback = [
bigquery.SchemaField("anomaly_id", "STRING"),
bigquery.SchemaField("is_true_positive", "BOOLEAN"),
bigquery.SchemaField("anomaly_type", "STRING")
]


# job_config = bigquery.LoadJobConfig(
#   schema=schema_for_anomaly,
#   source_format=bigquery.SourceFormat.CSV
#   )
# job_config.skip_leading_rows = 1

DATASET = 'ambri'

def main(event, context):
    blob_name = event['name']
    
    if not blob_name.startswith('model_output'):
      return 0

    source_bucket = event['bucket']
    gs_path = f'gs://{source_bucket}/{blob_name}'
  
    df = pd.read_csv(gs_path)
    df['cell_id'] = df['cell_id'].str.lower()
    df['anomaly_id'] = df['anomaly_id'].str.lower()
    df[["start_ts","end_ts","cell_last_known_ts"]] = df[["start_ts","end_ts","cell_last_known_ts"]].applymap(lambda x: datetime.datetime.fromtimestamp(x))
    # table = bq.get_table("coherence-proto.ambri.anomaly")
    # bq.insert_rows_from_dataframe(table, df)
    bq.bq_add_dataframe_to_table(df, DATASET, 'anomaly', schema_for_anomaly)
    
    df = df[['anomaly_id']]
    df['is_true_positive'] = None
    df['anomaly_type'] = None
    # table = bq.get_table("coherence-proto.ambri.anomaly_user_feedback")
    # bq.insert_rows_from_dataframe(table, df)
    bq.bq_add_dataframe_to_table(df, DATASET, 'anomaly_user_feedback', schema_for_feedback)
    