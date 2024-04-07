# CODE TO TRIGGER DATAFLOW FUNCTION EVERY TIME A NEW FILE IS UPLOADED TO GCS BUCKET

from googleapiclient.discovery import build

def trigger_df_job(cloud_event, env):
    service = build('dataflow', 'v1b3')
    project = 'bigquery-demo-415906'
    
    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"
    
    template_body = {
        'jobName': 'bq-load',   # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://bucket-dataflow-metadata/udf.js",
        "JSONPath": "gs://bucket-dataflow-metadata/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "bigquery-demo-415906:cricket_dataset.icc_odi_ranking",
        "inputFilePattern": "gs://bat-ranking-data/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://bucket-dataflow-metadata",
        }
    }
    
    request = service.projects().templates().launch(projectId = project, gcsPath = template_path, body = template_body,)
    response = request.execute()
    print(response)
