import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
#from apache_beam.io.gcp.gcsio import GcsIO

#export GOOGLE_APPLICATION_CREDENTIALS=gcp_service_account.json

parser = argparse.ArgumentParser()

#describes arguments which will be passed in CLI / airflow job
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

standard_processing = (
	pipeline_ex1
	| 'Read lines' >> beam.io.ReadFromText(input_loc)
	| 'write lines' >> beam.io.writeToFile(output_loc)
	)

pipeline_ex1.run()