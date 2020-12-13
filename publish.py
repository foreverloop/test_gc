import os
import time
from google.cloud import pubsub_v1

if __name__ == "__main__":
	project = "project-name"

	pubsub_topic = "projects/projectname/topics/topic"

	path_service_account = "local/sa.json"

	os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_service_account

	input_file = "counts.csv"

	publisher = pubsub_v1.PublisherClient()

	with open(input_file, 'rb') as ifp:
		header = ifp.readline()

		for line in ifp:
			event_data = line
			print('publishing {0} to {1}'.format(event_data, pubsub_topic))
			publisher.publish(pubsub_topic, event_data)
			time.sleep(2)