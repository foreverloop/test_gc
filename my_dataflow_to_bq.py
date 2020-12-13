import argparse
import apache_beam as beam
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

#from apache_beam.io.gcp.gcsio import GcsIO
#export GOOGLE_APPLICATION_CREDENTIALS=gcp_service_account.json

parser = argparse.ArgumentParser()

#describes arguments which will be passed in CLI (or from airflow)
parser.add_arguments('--input',
	dest='input',
	required=True,
	help='Input file to process.')

parser.add_arguments('--output',
	dest='output',
	required=True,
	help='Output file to write results to.')

path_args, pipeline_args = parser.parse_known_args()

input_loc = path_args.input
output_loc = path_args.output

options = PipelineOptions(pipeline_args)

pipeline_ex1 = beam.Pipeline()

send_data = (
	pipeline_ex1
	| 'Read lines' >> beam.io.ReadFromText(input_loc)
	)

client = bigquery.Client()

dataset_id = "project_id.dataset_name_to_create"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "EU"
dataset.description = "test_dataset"
#set expiration etc also if required

dataset_ref = client.create_dataset(dataset, timeout=30)

def row_to_json(csv_str):
	fields = csv_str.split(',')

	#general pattern to convert
	json_str = {"id":fields[0],
				"date":fields[1],
				"val0":fields[2],
				"val1":fields[3],
				"val2":fields[4]}

	return json_str

#likely whatever data we're using needs a date and a timestamp
table_schema = "id:INT64,date:DATE,val0:INT64,val1:INT64,val2:FLOAT64"

(send_data
	| 'convert to JSON' >> beam.Map(row_to_json)
	| 'write to BQ' >> beam.io.writeToBigQuery(
		output_loc,
		schema=table_schema,
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
		additional_bq_parameters={'timePartitioning' : {'type':'DAY'}}
		)
)

pipeline_status = pipeline_ex1.run()

if pipeline_status.state == PipelineState.DONE:
	print('finished successfully')
else:
	print('error running pipeline')