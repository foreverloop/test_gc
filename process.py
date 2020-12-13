import apache_beam as beam
from apache_beam.options.pipeline_options import pipeline_options
import os
from apache_beam import window

service_account_path = "local/sa.json"

print("service_account file:",service_account_path)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_path

input_subscription = "projects/project/subscriptions/subscription1"

output_topic = "projects/project/topic/topic2"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

output_file = 'outputs/part'

pubsub_data = (
				p
				| 'read from pubsub' >> beam.io.ReadFromPubSub(subscription=input_subscription)
				| 'write to pubsub' >> beam.io.WriteToPubSub(output_topic)
	)

result = p.run()
result.wait_until_finish()